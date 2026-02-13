use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Write};
use std::sync::Arc;

use crate::storage::{
    IndexedVcfReader, VcfLocalReader, VcfRecordFields, VcfRemoteReader, open_local_vcf_sync,
};
use crate::table_provider::{format_to_arrow_type, info_to_arrow_type};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, Float64Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::genomic_filter::GenomicRegion;
use datafusion_bio_format_core::partition_balancer::PartitionAssignment;
use datafusion_bio_format_core::record_filter::evaluate_record_filters;
use datafusion_bio_format_core::{
    object_storage::{
        CompressionType, ObjectStorageOptions, StorageType, get_compression_type, get_storage_type,
    },
    table_utils::{OptionalField, builders_to_arrays},
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{StreamExt, TryStreamExt};
use log::{debug, info};
use noodles_vcf::Header;
use noodles_vcf::header::{Formats, Infos};
use noodles_vcf::variant::Record;
use noodles_vcf::variant::record::info::field::{Value, value::Array as ValueArray};
use noodles_vcf::variant::record::samples::series::value::genotype::Phasing;
use noodles_vcf::variant::record::{AlternateBases, Filters, Ids, ReferenceBases, Samples};
use std::str;

/// Joins an iterator of displayable items into a reusable buffer with a separator,
/// avoiding intermediate Vec allocation.
fn join_into<I, D>(buf: &mut String, iter: I, sep: char)
where
    I: Iterator<Item = D>,
    D: std::fmt::Display,
{
    buf.clear();
    let mut first = true;
    for item in iter {
        if !first {
            buf.push(sep);
        }
        first = false;
        let _ = write!(buf, "{}", item);
    }
}

/// Constructs a DataFusion RecordBatch from VCF variant data.
///
/// This function assembles core VCF columns (chrom, pos, ref, alt, etc.), INFO fields,
/// and FORMAT fields into a RecordBatch for execution. It supports projection pushdown
/// to optimize queries.
///
/// # Arguments
///
/// * `schema` - The target Arrow schema
/// * `chroms` - Chromosome names
/// * `poss` - Variant start positions (0-based)
/// * `pose` - Variant end positions (0-based, inclusive)
/// * `ids` - Variant identifiers (semicolon-separated if multiple)
/// * `refs` - Reference bases
/// * `alts` - Alternate bases (pipe-separated if multiple)
/// * `quals` - Quality scores (optional)
/// * `filters` - FILTER field values (semicolon-separated)
/// * `infos` - Optional arrow arrays for INFO fields
/// * `formats` - Optional arrow arrays for FORMAT fields (per-sample)
/// * `num_info_fields` - Number of INFO fields (used for projection index calculation)
/// * `projection` - Optional list of column indices to project (None = all columns)
///
/// # Returns
///
/// A DataFusion RecordBatch with the specified data
///
/// # Errors
///
/// Returns an error if the RecordBatch cannot be created
#[allow(clippy::too_many_arguments)]
pub fn build_record_batch(
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
    formats: Option<&Vec<Arc<dyn Array>>>,
    num_info_fields: usize,
    projection: &Option<Vec<usize>>,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    let make_chrom = || {
        Arc::new(StringArray::from_iter_values(
            chroms.iter().map(|s| s.as_str()),
        )) as Arc<dyn Array>
    };
    let make_start =
        || Arc::new(UInt32Array::from_iter_values(poss.iter().copied())) as Arc<dyn Array>;
    let make_end =
        || Arc::new(UInt32Array::from_iter_values(pose.iter().copied())) as Arc<dyn Array>;
    let make_id = || {
        Arc::new(StringArray::from_iter_values(
            ids.iter().map(|s| s.as_str()),
        )) as Arc<dyn Array>
    };
    let make_ref = || {
        Arc::new(StringArray::from_iter_values(
            refs.iter().map(|s| s.as_str()),
        )) as Arc<dyn Array>
    };
    let make_alt = || {
        Arc::new(StringArray::from_iter_values(
            alts.iter().map(|s| s.as_str()),
        )) as Arc<dyn Array>
    };
    let make_qual = || Arc::new(Float64Array::from_iter(quals.iter().copied())) as Arc<dyn Array>;
    let make_filter = || {
        Arc::new(StringArray::from_iter_values(
            filters.iter().map(|s| s.as_str()),
        )) as Arc<dyn Array>
    };

    // Column index layout:
    // 0-7: core fields (chrom, start, end, id, ref, alt, qual, filter)
    // 8 to (8 + num_info_fields - 1): INFO fields
    // (8 + num_info_fields) onwards: FORMAT fields (per-sample)
    let format_start_idx = 8 + num_info_fields;

    let arrays = match projection {
        None => {
            let mut arrays: Vec<Arc<dyn Array>> = vec![
                make_chrom(),
                make_start(),
                make_end(),
                make_id(),
                make_ref(),
                make_alt(),
                make_qual(),
                make_filter(),
            ];
            if let Some(info_arrays) = infos {
                arrays.extend_from_slice(info_arrays);
            }
            if let Some(format_arrays) = formats {
                arrays.extend_from_slice(format_arrays);
            }
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(proj_ids.len());
            if proj_ids.is_empty() {
                // For empty projections (COUNT(*)), return an empty vector
                // The schema should already be empty from the table provider
            } else {
                for &i in proj_ids {
                    match i {
                        0 => arrays.push(make_chrom()),
                        1 => arrays.push(make_start()),
                        2 => arrays.push(make_end()),
                        3 => arrays.push(make_id()),
                        4 => arrays.push(make_ref()),
                        5 => arrays.push(make_alt()),
                        6 => arrays.push(make_qual()),
                        7 => arrays.push(make_filter()),
                        idx if idx < format_start_idx => {
                            // INFO field
                            if let Some(info_arrays) = infos {
                                arrays.push(info_arrays[idx - 8].clone());
                            }
                        }
                        idx => {
                            // FORMAT field
                            if let Some(format_arrays) = formats {
                                arrays.push(format_arrays[idx - format_start_idx].clone());
                            }
                        }
                    }
                }
            }
            arrays
        }
    };

    // For empty projections (COUNT(*)), we need to specify row count
    if arrays.is_empty() {
        let options = datafusion::arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(record_count));
        RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
    } else {
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
    }
}

/// Iterates all INFO fields in a single pass using `info.iter(header)` and dispatches
/// via HashMap, avoiding the O(N*M) cost of calling `info.get(header, key)` per field.
fn load_infos_single_pass(
    record: &dyn Record,
    header: &Header,
    info_data_types: &[DataType],
    info_builders: &mut [OptionalField],
    info_name_to_index: &HashMap<String, usize>,
    info_populated: &mut [bool],
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let num_builders = info_builders.len();
    if num_builders == 0 {
        return Ok(());
    }

    // Reset populated tracking
    info_populated.iter_mut().for_each(|v| *v = false);

    let info = record.info();
    for result in info.iter(header) {
        let (key, value) = result.map_err(|e| {
            datafusion::arrow::error::ArrowError::InvalidArgumentError(format!(
                "Error reading INFO field: {}",
                e
            ))
        })?;

        if let Some(&idx) = info_name_to_index.get(key) {
            info_populated[idx] = true;
            let data_type = &info_data_types[idx];
            let builder = &mut info_builders[idx];

            match value {
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
                _ => {
                    return Err(datafusion::arrow::error::ArrowError::InvalidArgumentError(
                        format!("Unsupported INFO value type for field '{}'", key),
                    ));
                }
            }
        }
    }

    // Backfill nulls for fields not present in this record
    for idx in 0..num_builders {
        if !info_populated[idx] {
            if info_data_types[idx] == DataType::Boolean {
                info_builders[idx].append_boolean(false)?;
            } else {
                info_builders[idx].append_null()?;
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

#[allow(clippy::too_many_arguments)]
async fn get_local_vcf(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    // let mut count: usize = 0;
    let mut batch_num = 0;
    let schema = Arc::clone(&schema_ref);
    let file_path = file_path.clone();
    let mut reader = VcfLocalReader::new(
        file_path.clone(),
        object_storage_options.unwrap_or_default(),
    )
    .await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let formats = header.formats();
    let mut record_num = 0;
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields, infos, &mut info_builders);
    let num_info_fields = info_builders.0.len();

    // Build INFO name→index HashMap for single-pass iteration
    // Uses owned String keys (~20-30 short strings cloned once per query) to avoid
    // borrow conflicts with the mutable builders and async generator lifetime issues
    let info_name_to_index: HashMap<String, usize> = info_builders
        .0
        .iter()
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();
    let mut info_populated = vec![false; num_info_fields];

    // Initialize FORMAT builders
    let mut format_builders: FormatBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    set_format_builders(
        batch_size,
        format_fields,
        &sample_names,
        formats,
        &mut format_builders,
    );
    let has_format_fields = !format_builders.0.is_empty();

    // Build FORMAT field name→index HashMap for single-pass iteration
    // Uses owned String keys (~5-10 short strings cloned once per query)
    let num_format_fields = if sample_names.is_empty() || format_builders.0.is_empty() {
        0
    } else {
        format_builders.0.len() / sample_names.len()
    };
    let format_field_to_index: HashMap<String, usize> = format_builders
        .1
        .iter()
        .take(num_format_fields)
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();
    let mut format_populated = vec![false; num_format_fields];
    let sample_count = sample_names.len();

    // Determine which core fields are needed based on projection
    let needs_chrom = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&0));
    let needs_start = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&1));
    let needs_end = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&2));
    let needs_id = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&3));
    let needs_ref = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&4));
    let needs_alt = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&5));
    let needs_qual = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&6));
    let needs_filter = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&7));
    let needs_any_info = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 8 && i < 8 + num_info_fields));
    let needs_any_format = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 8 + num_info_fields));

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

    let has_residual_filters = !residual_filters.is_empty();

    let stream = try_stream! {
        let mut join_buf = String::with_capacity(64);

        let mut records = reader.read_records();
        // let iter_start_time = Instant::now();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Compute position fields needed for early filtering
            let start_pos_1based = record.variant_start().unwrap()?.get() as u32;
            let start_val = if coordinate_system_zero_based { start_pos_1based - 1 } else { start_pos_1based };

            // Apply residual filters early to skip records before accumulation
            if has_residual_filters {
                let end_val = get_variant_end(&record, &header);
                let fields = VcfRecordFields {
                    chrom: Some(record.reference_sequence_name().to_string()),
                    start: Some(start_val),
                    end: Some(end_val),
                };
                if !evaluate_record_filters(&fields, &residual_filters) {
                    continue;
                }
            }

            if needs_chrom { chroms.push(record.reference_sequence_name().to_string()); }
            if needs_start { poss.push(start_val); }
            if needs_end { pose.push(get_variant_end(&record, &header)); }
            if needs_id { join_into(&mut join_buf, record.ids().iter(), ';'); ids.push(join_buf.clone()); }
            if needs_ref { refs.push(record.reference_bases().to_string()); }
            if needs_alt { join_into(&mut join_buf, record.alternate_bases().iter().map(|v| v.unwrap_or(".")), '|'); alts.push(join_buf.clone()); }
            if needs_qual { quals.push(record.quality_score().transpose()?.map(|v| v as f64)); }
            if needs_filter { join_into(&mut join_buf, record.filters().iter(&header).map(|v| v.unwrap_or(".")), ';'); filters.push(join_buf.clone()); }
            if needs_any_info { load_infos_single_pass(&record, &header, &info_builders.1, &mut info_builders.2, &info_name_to_index, &mut info_populated)?; }
            if has_format_fields && needs_any_format {
                load_formats_single_pass(&record, &header, sample_count, &format_builders.2, &mut format_builders.3, &format_field_to_index, num_format_fields, &mut format_populated)?;
            }
            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let info_arrays = if needs_any_info {
                    Some(builders_to_arrays(&mut info_builders.2))
                } else {
                    None
                };
                let format_arrays = if has_format_fields && needs_any_format {
                    Some(builders_to_arrays(&mut format_builders.3))
                } else {
                    None
                };
                let batch = build_record_batch(
                    schema.clone(),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    info_arrays.as_ref(),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
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
        if record_num > 0 && record_num % batch_size != 0 {
            let info_arrays = if needs_any_info {
                Some(builders_to_arrays(&mut info_builders.2))
            } else {
                None
            };
            let format_arrays = if has_format_fields && needs_any_format {
                Some(builders_to_arrays(&mut format_builders.3))
            } else {
                None
            };
            let batch = build_record_batch(
                schema.clone(),
                &chroms,
                &poss,
                &pose,
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
                info_arrays.as_ref(),
                format_arrays.as_ref(),
                num_info_fields,
                &projection,
                record_num % batch_size,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

/// Reads a local VCF file using a synchronous thread with buffer reuse.
///
/// Uses `read_record(&mut record)` to reuse a single record buffer across reads,
/// avoiding per-record clone allocations. Sends batches via a bounded channel for
/// backpressure. Falls back to the async `get_local_vcf` for GZIP compression.
#[allow(clippy::too_many_arguments)]
async fn get_local_vcf_sync(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    compression_type: CompressionType,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<
        Result<RecordBatch, datafusion::arrow::error::ArrowError>,
    >(2);

    std::thread::spawn(move || {
        let read_and_send = || -> Result<(), DataFusionError> {
            let (mut reader, header) = open_local_vcf_sync(&file_path, compression_type)
                .map_err(|e| DataFusionError::Execution(format!("Failed to open VCF: {}", e)))?;

            let infos = header.infos();
            let formats = header.formats();

            // Initialize INFO builders
            let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
                (Vec::new(), Vec::new(), Vec::new());
            set_info_builders(batch_size, info_fields, infos, &mut info_builders);
            let num_info_fields = info_builders.0.len();

            let info_name_to_index: HashMap<String, usize> = info_builders
                .0
                .iter()
                .enumerate()
                .map(|(i, name)| (name.clone(), i))
                .collect();
            let mut info_populated = vec![false; num_info_fields];

            // Initialize FORMAT builders
            let mut format_builders: FormatBuilders =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            set_format_builders(
                batch_size,
                format_fields,
                &sample_names,
                formats,
                &mut format_builders,
            );
            let has_format_fields = !format_builders.0.is_empty();

            let num_format_fields = if sample_names.is_empty() || format_builders.0.is_empty() {
                0
            } else {
                format_builders.0.len() / sample_names.len()
            };
            let format_field_to_index: HashMap<String, usize> = format_builders
                .1
                .iter()
                .take(num_format_fields)
                .enumerate()
                .map(|(i, name)| (name.clone(), i))
                .collect();
            let mut format_populated = vec![false; num_format_fields];
            let sample_count = sample_names.len();

            // Projection flags
            let needs_chrom = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&0));
            let needs_start = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&1));
            let needs_end = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&2));
            let needs_id = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&3));
            let needs_ref = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&4));
            let needs_alt = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&5));
            let needs_qual = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&6));
            let needs_filter = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&7));
            let needs_any_info = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 8 && i < 8 + num_info_fields));
            let needs_any_format = projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 8 + num_info_fields));

            // Initialize accumulators
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
            let mut join_buf = String::with_capacity(64);

            let has_residual_filters = !residual_filters.is_empty();
            let mut record = noodles_vcf::Record::default();
            let mut record_num = 0usize;

            loop {
                match reader.read_record(&mut record) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let start_pos_1based = record
                            .variant_start()
                            .ok_or_else(|| {
                                DataFusionError::Execution("Missing variant start".to_string())
                            })?
                            .map_err(|e| {
                                DataFusionError::Execution(format!("VCF position error: {}", e))
                            })?
                            .get() as u32;
                        let start_val = if coordinate_system_zero_based {
                            start_pos_1based - 1
                        } else {
                            start_pos_1based
                        };

                        // Apply residual filters early
                        if has_residual_filters {
                            let end_val = get_variant_end(&record, &header);
                            let fields = VcfRecordFields {
                                chrom: Some(record.reference_sequence_name().to_string()),
                                start: Some(start_val),
                                end: Some(end_val),
                            };
                            if !evaluate_record_filters(&fields, &residual_filters) {
                                continue;
                            }
                        }

                        if needs_chrom {
                            chroms.push(record.reference_sequence_name().to_string());
                        }
                        if needs_start {
                            poss.push(start_val);
                        }
                        if needs_end {
                            pose.push(get_variant_end(&record, &header));
                        }
                        if needs_id {
                            join_into(&mut join_buf, record.ids().iter(), ';');
                            ids.push(join_buf.clone());
                        }
                        if needs_ref {
                            refs.push(record.reference_bases().to_string());
                        }
                        if needs_alt {
                            join_into(
                                &mut join_buf,
                                record.alternate_bases().iter().map(|v| v.unwrap_or(".")),
                                '|',
                            );
                            alts.push(join_buf.clone());
                        }
                        if needs_qual {
                            quals.push(
                                record
                                    .quality_score()
                                    .transpose()
                                    .map_err(|e| {
                                        DataFusionError::Execution(format!("VCF qual error: {}", e))
                                    })?
                                    .map(|v| v as f64),
                            );
                        }
                        if needs_filter {
                            join_into(
                                &mut join_buf,
                                record.filters().iter(&header).map(|v| v.unwrap_or(".")),
                                ';',
                            );
                            filters.push(join_buf.clone());
                        }
                        if needs_any_info {
                            load_infos_single_pass(
                                &record,
                                &header,
                                &info_builders.1,
                                &mut info_builders.2,
                                &info_name_to_index,
                                &mut info_populated,
                            )
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        }
                        if has_format_fields && needs_any_format {
                            load_formats_single_pass(
                                &record,
                                &header,
                                sample_count,
                                &format_builders.2,
                                &mut format_builders.3,
                                &format_field_to_index,
                                num_format_fields,
                                &mut format_populated,
                            )
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        }

                        record_num += 1;

                        if record_num % batch_size == 0 {
                            debug!("Record number: {}", record_num);
                            let info_arrays = if needs_any_info {
                                Some(builders_to_arrays(&mut info_builders.2))
                            } else {
                                None
                            };
                            let format_arrays = if has_format_fields && needs_any_format {
                                Some(builders_to_arrays(&mut format_builders.3))
                            } else {
                                None
                            };
                            let batch = build_record_batch(
                                Arc::clone(&schema),
                                &chroms,
                                &poss,
                                &pose,
                                &ids,
                                &refs,
                                &alts,
                                &quals,
                                &filters,
                                info_arrays.as_ref(),
                                format_arrays.as_ref(),
                                num_info_fields,
                                &projection,
                                batch_size,
                            )?;

                            loop {
                                match tx.try_send(Ok(batch.clone())) {
                                    Ok(()) => break,
                                    Err(e) if e.is_disconnected() => return Ok(()),
                                    Err(_) => std::thread::yield_now(),
                                }
                            }

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
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!("VCF read error: {}", e)));
                    }
                }
            }

            // Remaining records
            if record_num > 0 && record_num % batch_size != 0 {
                let info_arrays = if needs_any_info {
                    Some(builders_to_arrays(&mut info_builders.2))
                } else {
                    None
                };
                let format_arrays = if has_format_fields && needs_any_format {
                    Some(builders_to_arrays(&mut format_builders.3))
                } else {
                    None
                };
                let batch = build_record_batch(
                    Arc::clone(&schema),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    info_arrays.as_ref(),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
                    record_num % batch_size,
                )?;
                loop {
                    match tx.try_send(Ok(batch.clone())) {
                        Ok(()) => break,
                        Err(e) if e.is_disconnected() => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }

            debug!("Local VCF sync scan: {} records", record_num);
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(datafusion::arrow::error::ArrowError::ExternalError(
                Box::new(e),
            )));
        }
    });

    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}

#[allow(clippy::too_many_arguments)]
async fn get_remote_vcf_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader = VcfRemoteReader::new(
        file_path.clone(),
        object_storage_options.unwrap_or_default(),
    )
    .await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let formats = header.formats();
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields, infos, &mut info_builders);
    let num_info_fields = info_builders.0.len();

    // Build INFO name→index HashMap for single-pass iteration
    // Uses owned String keys (~20-30 short strings cloned once per query) to avoid
    // borrow conflicts with the mutable builders and async generator lifetime issues
    let info_name_to_index: HashMap<String, usize> = info_builders
        .0
        .iter()
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();
    let mut info_populated = vec![false; num_info_fields];

    // Initialize FORMAT builders
    let mut format_builders: FormatBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    set_format_builders(
        batch_size,
        format_fields,
        &sample_names,
        formats,
        &mut format_builders,
    );
    let has_format_fields = !format_builders.0.is_empty();

    // Build FORMAT field name→index HashMap for single-pass iteration
    // Uses owned String keys (~5-10 short strings cloned once per query)
    let num_format_fields = if sample_names.is_empty() || format_builders.0.is_empty() {
        0
    } else {
        format_builders.0.len() / sample_names.len()
    };
    let format_field_to_index: HashMap<String, usize> = format_builders
        .1
        .iter()
        .take(num_format_fields)
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();
    let mut format_populated = vec![false; num_format_fields];
    let sample_count = sample_names.len();

    // Determine which core fields are needed based on projection
    let needs_chrom = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&0));
    let needs_start = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&1));
    let needs_end = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&2));
    let needs_id = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&3));
    let needs_ref = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&4));
    let needs_alt = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&5));
    let needs_qual = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&6));
    let needs_filter = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.contains(&7));
    let needs_any_info = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 8 && i < 8 + num_info_fields));
    let needs_any_format = projection
        .as_ref()
        .is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 8 + num_info_fields));
    let has_residual_filters = !residual_filters.is_empty();

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut chroms: Vec<String> = if needs_chrom { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut poss: Vec<u32> = if needs_start { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut pose: Vec<u32> = if needs_end { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut ids: Vec<String> = if needs_id { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut refs: Vec<String> = if needs_ref { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut alts: Vec<String> = if needs_alt { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut quals: Vec<Option<f64>> = if needs_qual { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut filters: Vec<String> = if needs_filter { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut join_buf = String::with_capacity(64);

        debug!("Info fields: {:?}", info_builders);

        let mut record_num = 0;
        let mut batch_num = 0;


        // Process records one by one.

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Compute position fields needed for early filtering
            let start_pos_1based = record.variant_start().unwrap()?.get() as u32;
            let start_val = if coordinate_system_zero_based { start_pos_1based - 1 } else { start_pos_1based };

            // Apply residual filters early to skip records before accumulation
            if has_residual_filters {
                let end_val = get_variant_end(&record, &header);
                let fields = VcfRecordFields {
                    chrom: Some(record.reference_sequence_name().to_string()),
                    start: Some(start_val),
                    end: Some(end_val),
                };
                if !evaluate_record_filters(&fields, &residual_filters) {
                    continue;
                }
            }

            if needs_chrom { chroms.push(record.reference_sequence_name().to_string()); }
            if needs_start { poss.push(start_val); }
            if needs_end { pose.push(get_variant_end(&record, &header)); }
            if needs_id { join_into(&mut join_buf, record.ids().iter(), ';'); ids.push(join_buf.clone()); }
            if needs_ref { refs.push(record.reference_bases().to_string()); }
            if needs_alt { join_into(&mut join_buf, record.alternate_bases().iter().map(|v| v.unwrap_or(".")), '|'); alts.push(join_buf.clone()); }
            if needs_qual { quals.push(record.quality_score().transpose()?.map(|v| v as f64)); }
            if needs_filter { join_into(&mut join_buf, record.filters().iter(&header).map(|v| v.unwrap_or(".")), ';'); filters.push(join_buf.clone()); }
            if needs_any_info { load_infos_single_pass(&record, &header, &info_builders.1, &mut info_builders.2, &info_name_to_index, &mut info_populated)?; }
            if has_format_fields && needs_any_format {
                load_formats_single_pass(&record, &header, sample_count, &format_builders.2, &mut format_builders.3, &format_field_to_index, num_format_fields, &mut format_populated)?;
            }
            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let info_arrays = if needs_any_info {
                    Some(builders_to_arrays(&mut info_builders.2))
                } else {
                    None
                };
                let format_arrays = if has_format_fields && needs_any_format {
                    Some(builders_to_arrays(&mut format_builders.3))
                } else {
                    None
                };
                let batch = build_record_batch(
                    schema.clone(),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    info_arrays.as_ref(),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
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
        if record_num > 0 && record_num % batch_size != 0 {
            let info_arrays = if needs_any_info {
                Some(builders_to_arrays(&mut info_builders.2))
            } else {
                None
            };
            let format_arrays = if has_format_fields && needs_any_format {
                Some(builders_to_arrays(&mut format_builders.3))
            } else {
                None
            };
            let batch = build_record_batch(
                schema.clone(),
                &chroms,
                &poss,
                &pose,
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
                info_arrays.as_ref(),
                format_arrays.as_ref(),
                num_info_fields,
                &projection,
                record_num % batch_size,
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

/// Holds builders for per-sample FORMAT fields.
/// Format builders are organized as: for each sample, for each format field.
/// This matches the column ordering: sample1_field1, sample1_field2, sample2_field1, sample2_field2, etc.
type FormatBuilders = (Vec<String>, Vec<String>, Vec<DataType>, Vec<OptionalField>);

fn set_format_builders(
    batch_size: usize,
    format_fields: Option<Vec<String>>,
    sample_names: &[String],
    formats: &Formats,
    format_builders: &mut FormatBuilders,
) {
    // If format_fields is None, include all FORMAT fields from header
    let fields: Vec<String> = match format_fields {
        Some(tags) => tags,
        None => formats.keys().map(|k| k.to_string()).collect(),
    };
    for sample_name in sample_names {
        for f in &fields {
            let data_type = format_to_arrow_type(formats, f);
            let field = OptionalField::new(&data_type, batch_size).unwrap();
            format_builders.0.push(sample_name.clone()); // sample name
            format_builders.1.push(f.clone()); // format field name
            format_builders.2.push(data_type);
            format_builders.3.push(field);
        }
    }
}

/// Iterates all FORMAT fields per-sample in a single pass using `sample.iter(header)`,
/// avoiding the O(N*M) cost of calling `samples.select(header, field)` per field
/// and `series.get(header, sample_idx)` per sample.
#[allow(clippy::too_many_arguments)]
fn load_formats_single_pass(
    record: &dyn Record,
    header: &Header,
    sample_count: usize,
    format_data_types: &[DataType],
    format_builders: &mut [OptionalField],
    format_field_to_index: &HashMap<String, usize>,
    num_format_fields: usize,
    format_populated: &mut [bool],
) -> Result<(), datafusion::arrow::error::ArrowError> {
    use noodles_vcf::variant::record::samples::series::Value as SV;
    use noodles_vcf::variant::record::samples::series::value::Array as SamplesArray;

    let samples = match record.samples() {
        Ok(s) => s,
        Err(_) => {
            // If samples can't be read, append null for all format fields
            for builder in format_builders.iter_mut() {
                builder.append_null()?;
            }
            return Ok(());
        }
    };

    if num_format_fields == 0 {
        return Ok(());
    }

    for (sample_idx, sample) in samples.iter().enumerate().take(sample_count) {
        let base_builder_idx = sample_idx * num_format_fields;

        // Reset populated tracking for this sample
        format_populated.iter_mut().for_each(|v| *v = false);

        for result in sample.iter(header) {
            let (key, value) = result.map_err(|e| {
                datafusion::arrow::error::ArrowError::InvalidArgumentError(format!(
                    "Error reading FORMAT field: {}",
                    e
                ))
            })?;

            let local_idx = match format_field_to_index.get(key) {
                Some(&idx) => idx,
                None => continue,
            };

            format_populated[local_idx] = true;
            let builder_idx = base_builder_idx + local_idx;
            let builder = &mut format_builders[builder_idx];
            let data_type = &format_data_types[builder_idx];

            // Handle GT (genotype) specially - always convert to string
            if key == "GT" {
                match value {
                    Some(SV::Genotype(gt)) => {
                        let mut gt_str = String::new();
                        let mut first = true;
                        for (allele, phasing) in gt.iter().flatten() {
                            if !first {
                                match phasing {
                                    Phasing::Phased => gt_str.push('|'),
                                    Phasing::Unphased => gt_str.push('/'),
                                }
                            }
                            first = false;
                            match allele {
                                Some(a) => {
                                    let _ = write!(gt_str, "{}", a);
                                }
                                None => gt_str.push('.'),
                            }
                        }
                        builder.append_string(&gt_str)?;
                    }
                    _ => builder.append_null()?,
                }
                continue;
            }

            // Handle other FORMAT fields
            match value {
                Some(SV::Integer(v)) => builder.append_int(v)?,
                Some(SV::Float(v)) => builder.append_float(v)?,
                Some(SV::String(v)) => builder.append_string(&v)?,
                Some(SV::Character(c)) => builder.append_string(&c.to_string())?,
                Some(SV::Array(arr)) => {
                    match arr {
                        SamplesArray::Integer(values) => {
                            // Preserve nulls for proper allele alignment (e.g., AD=10,. -> [10, null])
                            let ints: Vec<Option<i32>> =
                                values.iter().map(|v| v.ok().flatten()).collect();
                            let all_null = ints.iter().all(|v| v.is_none());
                            if all_null {
                                builder.append_null()?;
                            } else if matches!(data_type, DataType::Int32) {
                                // Scalar type but got array - take first non-null value
                                if let Some(first) = ints.iter().find_map(|v| *v) {
                                    builder.append_int(first)?;
                                } else {
                                    builder.append_null()?;
                                }
                            } else {
                                builder.append_array_int_nullable(ints)?;
                            }
                        }
                        SamplesArray::Float(values) => {
                            let floats: Vec<Option<f32>> =
                                values.iter().map(|v| v.ok().flatten()).collect();
                            let all_null = floats.iter().all(|v| v.is_none());
                            if all_null {
                                builder.append_null()?;
                            } else if matches!(data_type, DataType::Float32) {
                                if let Some(first) = floats.iter().find_map(|v| *v) {
                                    builder.append_float(first)?;
                                } else {
                                    builder.append_null()?;
                                }
                            } else {
                                builder.append_array_float_nullable(floats)?;
                            }
                        }
                        SamplesArray::String(values) => {
                            let strings: Vec<Option<String>> = values
                                .iter()
                                .map(|v| v.ok().flatten().map(|s| s.to_string()))
                                .collect();
                            let all_null = strings.iter().all(|v| v.is_none());
                            if all_null {
                                builder.append_null()?;
                            } else if matches!(data_type, DataType::Utf8) {
                                if let Some(first) = strings.iter().find_map(|v| v.clone()) {
                                    builder.append_string(&first)?;
                                } else {
                                    builder.append_null()?;
                                }
                            } else {
                                builder.append_array_string_nullable(strings)?;
                            }
                        }
                        SamplesArray::Character(values) => {
                            let strings: Vec<Option<String>> = values
                                .iter()
                                .map(|v| v.ok().flatten().map(|c| c.to_string()))
                                .collect();
                            let all_null = strings.iter().all(|v| v.is_none());
                            if all_null {
                                builder.append_null()?;
                            } else if matches!(data_type, DataType::Utf8) {
                                if let Some(first) = strings.iter().find_map(|v| v.clone()) {
                                    builder.append_string(&first)?;
                                } else {
                                    builder.append_null()?;
                                }
                            } else {
                                builder.append_array_string_nullable(strings)?;
                            }
                        }
                    }
                }
                Some(SV::Genotype(_)) => {
                    // Genotype should have been handled above
                    builder.append_null()?;
                }
                None => builder.append_null()?,
            }
        }

        // Backfill nulls for FORMAT fields not present in this sample
        for local_idx in 0..num_format_fields {
            if !format_populated[local_idx] {
                format_builders[base_builder_idx + local_idx].append_null()?;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn get_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let opts = object_storage_options.clone().unwrap_or_default();
            let compression_type = get_compression_type(
                file_path.clone(),
                opts.compression_type.clone(),
                opts.clone(),
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to detect compression: {}", e))
            })?;

            if matches!(compression_type, CompressionType::GZIP) {
                // GZIP: fall back to the async stream-based reader
                let stream = get_local_vcf(
                    file_path.clone(),
                    schema.clone(),
                    batch_size,
                    info_fields,
                    format_fields,
                    sample_names,
                    projection,
                    object_storage_options,
                    coordinate_system_zero_based,
                    residual_filters,
                )
                .await?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
            } else {
                // BGZF / PLAIN: sync thread with buffer reuse
                get_local_vcf_sync(
                    file_path.clone(),
                    schema_ref,
                    batch_size,
                    info_fields,
                    format_fields,
                    sample_names,
                    projection,
                    coordinate_system_zero_based,
                    residual_filters,
                    compression_type,
                )
                .await
            }
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_vcf_stream(
                file_path.clone(),
                schema.clone(),
                batch_size,
                info_fields,
                format_fields,
                sample_names,
                projection,
                object_storage_options,
                coordinate_system_zero_based,
                residual_filters,
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
    pub(crate) sample_names: Vec<String>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    pub(crate) coordinate_system_zero_based: bool,
    /// Partition assignments for index-based reading (None = full scan)
    pub(crate) partition_assignments: Option<Vec<PartitionAssignment>>,
    /// Path to the index file (TBI/CSI)
    pub(crate) index_path: Option<String>,
    /// Residual filters for record-level evaluation
    pub(crate) residual_filters: Vec<Expr>,
}

impl Debug for VcfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VcfExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for VcfExec {
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
        write!(f, "VcfExec: projection=[{}]", proj_str)
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

        // Use indexed reading when partition assignments and index are available
        if let (Some(assignments), Some(index_path)) =
            (&self.partition_assignments, &self.index_path)
        {
            if partition < assignments.len() {
                let regions = assignments[partition].regions.clone();
                let file_path = self.file_path.clone();
                let index_path = index_path.clone();
                let projection = self.projection.clone();
                let coord_zero_based = self.coordinate_system_zero_based;
                let info_fields = self.info_fields.clone();
                let format_fields = self.format_fields.clone();
                let sample_names = self.sample_names.clone();
                let residual_filters = self.residual_filters.clone();

                let fut = get_indexed_vcf_stream(
                    file_path,
                    index_path,
                    regions,
                    schema.clone(),
                    batch_size,
                    projection,
                    coord_zero_based,
                    info_fields,
                    format_fields,
                    sample_names,
                    residual_filters,
                );
                let stream = futures::stream::once(fut).try_flatten();
                return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
            }
        }

        // Fallback: full scan (original path)
        let fut = get_stream(
            self.file_path.clone(),
            schema.clone(),
            batch_size,
            self.info_fields.clone(),
            self.format_fields.clone(),
            self.sample_names.clone(),
            self.projection.clone(),
            self.object_storage_options.clone(),
            self.coordinate_system_zero_based,
            self.residual_filters.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Build a noodles Region from a GenomicRegion.
fn build_noodles_region(region: &GenomicRegion) -> Result<noodles_core::Region, DataFusionError> {
    let region_str = match (region.start, region.end) {
        (Some(start), Some(end)) => format!("{}:{}-{}", region.chrom, start, end),
        (Some(start), None) => format!("{}:{}", region.chrom, start),
        (None, Some(end)) => format!("{}:1-{}", region.chrom, end),
        (None, None) => region.chrom.clone(),
    };

    region_str
        .parse::<noodles_core::Region>()
        .map_err(|e| DataFusionError::Execution(format!("Invalid region '{}': {}", region_str, e)))
}

/// Get a streaming RecordBatch stream from an indexed VCF file for one or more regions.
///
/// Uses `thread::spawn` + `mpsc::channel(2)` for streaming I/O with backpressure.
/// Each partition processes its assigned regions sequentially, keeping only ~3 batches
/// in memory at a time (constant memory regardless of data volume).
#[allow(clippy::too_many_arguments)]
async fn get_indexed_vcf_stream(
    file_path: String,
    index_path: String,
    regions: Vec<GenomicRegion>,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    residual_filters: Vec<Expr>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<
        Result<RecordBatch, datafusion::arrow::error::ArrowError>,
    >(2);

    std::thread::spawn(move || {
        let read_and_send = || -> Result<(), DataFusionError> {
            let mut indexed_reader =
                IndexedVcfReader::new(&file_path, &index_path).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to open indexed VCF: {}", e))
                })?;

            let header = indexed_reader.header().clone();
            let infos = header.infos();
            let formats = header.formats();

            // Initialize accumulators
            let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
            let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
            let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
            let mut ids: Vec<String> = Vec::with_capacity(batch_size);
            let mut refs: Vec<String> = Vec::with_capacity(batch_size);
            let mut alts: Vec<String> = Vec::with_capacity(batch_size);
            let mut quals: Vec<Option<f64>> = Vec::with_capacity(batch_size);
            let mut filters: Vec<String> = Vec::with_capacity(batch_size);
            let mut join_buf = String::with_capacity(64);

            // Initialize INFO builders
            let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
                (Vec::new(), Vec::new(), Vec::new());
            set_info_builders(batch_size, info_fields, infos, &mut info_builders);
            let num_info_fields = info_builders.0.len();

            // Build INFO name→index HashMap for single-pass iteration
            let info_name_to_index: HashMap<String, usize> = info_builders
                .0
                .iter()
                .enumerate()
                .map(|(i, name)| (name.clone(), i))
                .collect();
            let mut info_populated = vec![false; num_info_fields];

            // Initialize FORMAT builders
            let mut format_builders: FormatBuilders =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            set_format_builders(
                batch_size,
                format_fields,
                &sample_names,
                formats,
                &mut format_builders,
            );
            let has_format_fields = !format_builders.0.is_empty();

            // Build FORMAT field name→index HashMap for single-pass iteration
            let num_format_fields = if sample_names.is_empty() || format_builders.0.is_empty() {
                0
            } else {
                format_builders.0.len() / sample_names.len()
            };
            let format_field_to_index: HashMap<String, usize> = format_builders
                .1
                .iter()
                .take(num_format_fields)
                .enumerate()
                .map(|(i, name)| (name.clone(), i))
                .collect();
            let mut format_populated = vec![false; num_format_fields];
            let sample_count = sample_names.len();

            let mut total_records = 0usize;

            for region in &regions {
                // Skip unmapped_tail regions — not applicable to VCF
                if region.unmapped_tail {
                    continue;
                }

                // Sub-region bounds for deduplication (1-based, from partition balancer)
                let region_start_1based = region.start.map(|s| s as u32);
                let region_end_1based = region.end.map(|e| e as u32);

                let noodles_region = build_noodles_region(region)?;

                let records = indexed_reader.query(&noodles_region).map_err(|e| {
                    DataFusionError::Execution(format!("VCF region query failed: {}", e))
                })?;

                for result in records {
                    let record = result.map_err(|e| {
                        DataFusionError::Execution(format!("VCF record read error: {}", e))
                    })?;

                    let start_pos = record
                        .variant_start()
                        .ok_or_else(|| {
                            DataFusionError::Execution("Missing variant start".to_string())
                        })?
                        .map_err(|e| {
                            DataFusionError::Execution(format!("VCF position error: {}", e))
                        })?
                        .get() as u32;
                    let start_val = if coordinate_system_zero_based {
                        start_pos - 1
                    } else {
                        start_pos
                    };

                    // Skip records outside the sub-region bounds (TBI bins may return
                    // overlapping records at sub-region boundaries)
                    if let Some(rs) = region_start_1based {
                        if start_pos < rs {
                            continue;
                        }
                    }
                    if let Some(re) = region_end_1based {
                        if start_pos > re {
                            continue;
                        }
                    }

                    let end_val = get_variant_end(&record, &header);

                    // Apply residual filters
                    if !residual_filters.is_empty() {
                        let fields = VcfRecordFields {
                            chrom: Some(record.reference_sequence_name().to_string()),
                            start: Some(start_val),
                            end: Some(end_val),
                        };
                        if !evaluate_record_filters(&fields, &residual_filters) {
                            continue;
                        }
                    }

                    chroms.push(record.reference_sequence_name().to_string());
                    poss.push(start_val);
                    pose.push(end_val);
                    join_into(&mut join_buf, record.ids().iter(), ';');
                    ids.push(join_buf.clone());
                    refs.push(record.reference_bases().to_string());
                    join_into(
                        &mut join_buf,
                        record.alternate_bases().iter().map(|v| v.unwrap_or(".")),
                        '|',
                    );
                    alts.push(join_buf.clone());
                    quals.push(
                        record
                            .quality_score()
                            .transpose()
                            .map_err(|e| {
                                DataFusionError::Execution(format!("VCF qual error: {}", e))
                            })?
                            .map(|v| v as f64),
                    );
                    join_into(
                        &mut join_buf,
                        record.filters().iter(&header).map(|v| v.unwrap_or(".")),
                        ';',
                    );
                    filters.push(join_buf.clone());

                    load_infos_single_pass(
                        &record,
                        &header,
                        &info_builders.1,
                        &mut info_builders.2,
                        &info_name_to_index,
                        &mut info_populated,
                    )
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    if has_format_fields {
                        load_formats_single_pass(
                            &record,
                            &header,
                            sample_count,
                            &format_builders.2,
                            &mut format_builders.3,
                            &format_field_to_index,
                            num_format_fields,
                            &mut format_populated,
                        )
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    }

                    total_records += 1;

                    if total_records % batch_size == 0 {
                        let format_arrays = if has_format_fields {
                            Some(builders_to_arrays(&mut format_builders.3))
                        } else {
                            None
                        };
                        let batch = build_record_batch(
                            Arc::clone(&schema),
                            &chroms,
                            &poss,
                            &pose,
                            &ids,
                            &refs,
                            &alts,
                            &quals,
                            &filters,
                            Some(&builders_to_arrays(&mut info_builders.2)),
                            format_arrays.as_ref(),
                            num_info_fields,
                            &projection,
                            chroms.len(),
                        )?;

                        // Send batch with backpressure
                        loop {
                            match tx.try_send(Ok(batch.clone())) {
                                Ok(()) => break,
                                Err(e) if e.is_disconnected() => return Ok(()),
                                Err(_) => std::thread::yield_now(),
                            }
                        }

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
            }

            // Remaining records
            if !chroms.is_empty() {
                let format_arrays = if has_format_fields {
                    Some(builders_to_arrays(&mut format_builders.3))
                } else {
                    None
                };
                let batch = build_record_batch(
                    Arc::clone(&schema),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    Some(&builders_to_arrays(&mut info_builders.2)),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
                    chroms.len(),
                )?;
                loop {
                    match tx.try_send(Ok(batch.clone())) {
                        Ok(()) => break,
                        Err(e) if e.is_disconnected() => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }

            debug!(
                "Indexed VCF scan: {} records for {} regions",
                total_records,
                regions.len()
            );
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(datafusion::arrow::error::ArrowError::ExternalError(
                Box::new(e),
            )));
        }
    });

    // Stream batches from the channel
    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}
