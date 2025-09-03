use crate::storage::{GffLocalReader, GffRecordTrait, GffRemoteReader};
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
use std::collections::HashMap;
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
    attr_fields: &[String],
    attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) {
    for attr_name in attr_fields {
        let field = OptionalField::new(&DataType::Utf8, batch_size).unwrap();
        attribute_builders.0.push(attr_name.clone());
        attribute_builders.1.push(DataType::Utf8);
        attribute_builders.2.push(field);
    }
}

/// Parse GFF3 attributes string into key-value HashMap - OPTIMIZED
fn parse_gff_attributes(attributes_str: &str) -> HashMap<String, String> {
    // Early return for empty/null attributes - most common case
    if attributes_str.is_empty() || attributes_str == "." {
        return HashMap::new();
    }

    // Pre-allocate HashMap with estimated capacity to reduce allocations
    let estimated_pairs = attributes_str.matches(';').count() + 1;
    let mut attributes = HashMap::with_capacity(estimated_pairs);

    // Split by semicolon and parse key=value pairs - avoid intermediate collections
    for pair in attributes_str.split(';') {
        if pair.is_empty() {
            continue;
        }

        // Find '=' position - most critical operation
        if let Some(eq_pos) = pair.find('=') {
            let key = &pair[..eq_pos];
            let value = &pair[eq_pos + 1..];

            // Only do expensive operations when absolutely necessary
            let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                // Remove quotes - avoid string allocation until necessary
                value[1..value.len() - 1].to_string()
            } else if value.contains('%') {
                // Only do URL decoding if % is found
                value
                    .replace("%3B", ";")
                    .replace("%3D", "=")
                    .replace("%26", "&")
                    .replace("%2C", ",")
                    .replace("%09", "\t")
            } else {
                // Most common case - direct string conversion
                value.to_string()
            };

            // Insert with minimal string allocation
            attributes.insert(key.to_string(), decoded_value);
        }
    }

    attributes
}

/// Load attributes from parsed attribute HashMap for unnested attributes
fn load_attributes_unnest_from_map(
    attributes_map: &HashMap<String, String>,
    attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
    projection: Option<Vec<usize>>,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let projected_attribute_indices: Option<Vec<usize>> =
        projection.map(|p| p.into_iter().filter(|i| *i >= 8).map(|i| i - 8).collect());

    for i in 0..attribute_builders.2.len() {
        if let Some(indices) = &projected_attribute_indices {
            if !indices.contains(&i) {
                attribute_builders.2[i].append_null()?;
                continue;
            }
        }

        let name = &attribute_builders.0[i];
        let builder = &mut attribute_builders.2[i];

        if let Some(value) = attributes_map.get(name) {
            builder.append_string(value)?;
        } else {
            builder.append_null()?;
        }
    }
    Ok(())
}

/// Load attributes from parsed attribute HashMap for nested attributes
fn load_attributes_from_map(
    attributes_map: &HashMap<String, String>,
    builder: &mut Vec<OptionalField>,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let mut vec_attributes: Vec<Attribute> = Vec::with_capacity(attributes_map.len());

    for (tag, value) in attributes_map.iter() {
        vec_attributes.push(Attribute {
            tag: tag.clone(),
            value: Some(value.clone()),
        });
    }

    builder[0].append_array_struct(vec_attributes)?;
    Ok(())
}

#[allow(dead_code)] // TODO: Implement for fast/SIMD parsers
fn load_attributes_unnest(
    record: &RecordBuf,
    attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
    projection: Option<Vec<usize>>,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let projected_attribute_indices: Option<Vec<usize>> =
        projection.map(|p| p.into_iter().filter(|i| *i >= 8).map(|i| i - 8).collect());

    // OPTIMIZATION: Get attributes once outside the loop
    let attributes = record.attributes();

    for i in 0..attribute_builders.2.len() {
        if let Some(indices) = &projected_attribute_indices {
            if !indices.contains(&i) {
                attribute_builders.2[i].append_null()?;
                continue;
            }
        }

        let name = &attribute_builders.0[i];
        let builder = &mut attribute_builders.2[i];
        let value = attributes.get(name.as_ref());

        match value {
            Some(v) => {
                // OPTIMIZATION: Avoid unnecessary string allocations
                match v {
                    Value::String(s) => {
                        // Convert BString to String, then take reference
                        let s_str = s.to_string();
                        builder.append_string(&s_str)?;
                    }
                    Value::Array(a) => {
                        // OPTIMIZATION: Use String::with_capacity and write directly
                        if a.is_empty() {
                            builder.append_string("")?;
                        } else {
                            // Estimate capacity: average 10 chars per item + separators
                            let capacity = a.len() * 10 + (a.len() - 1);
                            let mut result = String::with_capacity(capacity);

                            for (idx, val) in a.iter().enumerate() {
                                if idx > 0 {
                                    result.push(',');
                                }
                                // Convert BString to str
                                result.push_str(&val.to_string());
                            }
                            builder.append_string(&result)?;
                        }
                    }
                }
            }
            None => builder.append_null()?,
        }
    }
    Ok(())
}

#[allow(dead_code)] // TODO: Implement for fast/SIMD parsers
fn load_attributes(
    record: &RecordBuf,
    builder: &mut Vec<OptionalField>,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let attributes = record.attributes();

    // OPTIMIZATION: Pre-allocate Vec with estimated capacity
    let estimated_capacity = attributes.as_ref().len();
    let mut vec_attributes: Vec<Attribute> = Vec::with_capacity(estimated_capacity);

    for (tag, value) in attributes.as_ref().iter() {
        debug!("Loading attribute: {} with value: {:?}", tag, value);
        match value {
            Value::String(v) => vec_attributes.push(Attribute {
                tag: tag.to_string(),       // Still need owned string for Attribute struct
                value: Some(v.to_string()), // Still need owned string for Attribute struct
            }),
            Value::Array(v) => {
                // OPTIMIZATION: Build string more efficiently
                if v.is_empty() {
                    vec_attributes.push(Attribute {
                        tag: tag.to_string(),
                        value: Some(String::new()),
                    });
                } else {
                    // Estimate capacity for joined string
                    let capacity = v.len() * 10 + (v.len() - 1) * 2; // 2 chars for ", "
                    let mut result = String::with_capacity(capacity);

                    for (idx, val) in v.iter().enumerate() {
                        if idx > 0 {
                            result.push_str(", ");
                        }
                        // Convert BString to str
                        result.push_str(&val.to_string());
                    }

                    vec_attributes.push(Attribute {
                        tag: tag.to_string(),
                        value: Some(result),
                    });
                }
            }
        }
    }
    builder[0].append_array_struct(vec_attributes)?;
    Ok(())
}

// OPTIMIZATION: Use static string constants to avoid repeated allocations
static STRAND_FORWARD: &str = "+";
static STRAND_REVERSE: &str = "-";
static STRAND_UNKNOWN: &str = "?";
static STRAND_NONE: &str = ".";

fn standardize_strand(strand: Strand) -> &'static str {
    match strand {
        Strand::Forward => STRAND_FORWARD,
        Strand::Reverse => STRAND_REVERSE,
        Strand::Unknown => STRAND_UNKNOWN,
        Strand::None => STRAND_NONE,
    }
}

fn standardize_phase(phase: Option<Phase>) -> Option<u32> {
    phase.map(|p| match p {
        Phase::Zero => 0,
        Phase::One => 1,
        Phase::Two => 2,
    })
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
    let reader = GffRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await?;

    // Determine which core GFF fields we need to parse based on projection
    let needs_chrom = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_start = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_end = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_type = projection.as_ref().map_or(true, |proj| proj.contains(&3));
    let needs_source = projection.as_ref().map_or(true, |proj| proj.contains(&4));
    let needs_score = projection.as_ref().map_or(true, |proj| proj.contains(&5));
    let needs_strand = projection.as_ref().map_or(true, |proj| proj.contains(&6));
    let needs_phase = projection.as_ref().map_or(true, |proj| proj.contains(&7));

    debug!(
        "GFF remote projection {:?}: needs_chrom={}, needs_end={}, needs_type={}, needs_source={}",
        projection, needs_chrom, needs_end, needs_type, needs_source
    );

    //unnest builder
    let mut attribute_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) = (
        Vec::<String>::new(),
        Vec::<DataType>::new(),
        Vec::<OptionalField>::new(),
    );

    let unnest_enable = match &attr_fields {
        Some(attrs) => {
            set_attribute_builders(batch_size, attrs, &mut attribute_builders);
            true
        }
        None => false,
    };

    //nested builder - always create but only use when needed
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
        // Create vectors for accumulating record data only for needed fields.
        let mut chroms: Vec<String> = if needs_chrom { Vec::<String>::with_capacity(batch_size) } else { Vec::<String>::new() };
        let mut poss: Vec<u32> = if needs_start { Vec::<u32>::with_capacity(batch_size) } else { Vec::<u32>::new() };
        let mut pose: Vec<u32> = if needs_end { Vec::<u32>::with_capacity(batch_size) } else { Vec::<u32>::new() };
        let mut ty: Vec<String> = if needs_type { Vec::<String>::with_capacity(batch_size) } else { Vec::<String>::new() };
        let mut source: Vec<String> = if needs_source { Vec::<String>::with_capacity(batch_size) } else { Vec::<String>::new() };
        let mut scores: Vec<Option<f32>> = if needs_score { Vec::<Option<f32>>::with_capacity(batch_size) } else { Vec::<Option<f32>>::new() };
        let mut strand: Vec<String> = if needs_strand { Vec::<String>::with_capacity(batch_size) } else { Vec::<String>::new() };
        let mut phase: Vec<Option<u32>> = if needs_phase { Vec::<Option<u32>>::with_capacity(batch_size) } else { Vec::<Option<u32>>::new() };

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.

        let mut records = reader.read_records();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_chrom {
                chroms.push(record.reference_sequence_name().to_string());
            }
            if needs_start {
                poss.push(record.start().get() as u32);
            }
            if needs_end {
                pose.push(record.end().get() as u32);
            }
            if needs_type {
                ty.push(record.ty().to_string());
            }
            if needs_source {
                source.push(record.source().to_string());
            }
            if needs_score {
                scores.push(record.score());
            }
            if needs_strand {
                strand.push(standardize_strand(record.strand()).to_string());
            }
            if needs_phase {
                phase.push(standardize_phase(record.phase()));
            }

            // TODO: Implement attribute loading for UnifiedGffRecord
            // For now, we'll skip attribute processing to get the basic flow working
            if unnest_enable && !attribute_builders.0.is_empty() {
                // For each attribute field, append null for now
                for builder in &mut attribute_builders.2 {
                    builder.append_null()?;
                }
            } else if !unnest_enable {
                // Always append null to maintain schema consistency
                builder[0].append_null()?;
            }

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let attribute_arrays = if unnest_enable {
                    Some(builders_to_arrays(&mut attribute_builders.2))
                } else {
                    Some(builders_to_arrays(&mut builder))
                };
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ty,
                    &source,
                    &scores,
                    &strand,
                    &phase,
                    attribute_arrays.as_ref(),
                    projection.clone(),
                    needs_chrom,
                    needs_start,
                    needs_end,
                    needs_type,
                    needs_source,
                    needs_score,
                    needs_strand,
                    needs_phase,
                    batch_size,
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
        let remaining_records = record_num % batch_size;
        if remaining_records != 0 {
            let batch = build_record_batch_optimized(
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
                needs_chrom,
                needs_start,
                needs_end,
                needs_type,
                needs_source,
                needs_score,
                needs_strand,
                needs_phase,
                remaining_records,
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
    // Determine which core GFF fields we need to parse based on projection
    let needs_chrom = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_start = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_end = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_type = projection.as_ref().map_or(true, |proj| proj.contains(&3));
    let needs_source = projection.as_ref().map_or(true, |proj| proj.contains(&4));
    let needs_score = projection.as_ref().map_or(true, |proj| proj.contains(&5));
    let needs_strand = projection.as_ref().map_or(true, |proj| proj.contains(&6));
    let needs_phase = projection.as_ref().map_or(true, |proj| proj.contains(&7));

    let mut chroms: Vec<String> = if needs_chrom {
        Vec::<String>::with_capacity(batch_size)
    } else {
        Vec::<String>::new()
    };
    let mut poss: Vec<u32> = if needs_start {
        Vec::<u32>::with_capacity(batch_size)
    } else {
        Vec::<u32>::new()
    };
    let mut pose: Vec<u32> = if needs_end {
        Vec::<u32>::with_capacity(batch_size)
    } else {
        Vec::<u32>::new()
    };
    let mut ty: Vec<String> = if needs_type {
        Vec::<String>::with_capacity(batch_size)
    } else {
        Vec::<String>::new()
    };
    let mut source: Vec<String> = if needs_source {
        Vec::<String>::with_capacity(batch_size)
    } else {
        Vec::<String>::new()
    };
    let mut scores: Vec<Option<f32>> = if needs_score {
        Vec::<Option<f32>>::with_capacity(batch_size)
    } else {
        Vec::<Option<f32>>::new()
    };
    let mut strand: Vec<String> = if needs_strand {
        Vec::<String>::with_capacity(batch_size)
    } else {
        Vec::<String>::new()
    };
    let mut phase: Vec<Option<u32>> = if needs_phase {
        Vec::<Option<u32>>::with_capacity(batch_size)
    } else {
        Vec::<Option<u32>>::new()
    };

    // let mut count: usize = 0;
    let mut batch_num = 0;
    let file_path = file_path.clone();
    let thread_num = thread_num.unwrap_or(1);
    let reader = GffLocalReader::new(
        file_path.clone(),
        thread_num,
        object_storage_options.unwrap(),
    )
    .await?;

    // Convert to sync iterator for better performance
    let sync_iter = reader.into_sync_iterator();
    //unnest builder
    let mut attribute_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) = (
        Vec::<String>::new(),
        Vec::<DataType>::new(),
        Vec::<OptionalField>::new(),
    );

    let unnest_enable = match &attr_fields {
        Some(attrs) => {
            set_attribute_builders(batch_size, attrs, &mut attribute_builders);
            true
        }
        None => false,
    };

    //nested builder - always create but only use when needed
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

        // Process records synchronously using iterator
        for result in sync_iter {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_chrom {
                chroms.push(record.reference_sequence_name());
            }
            if needs_start {
                poss.push(record.start());
            }
            if needs_end {
                pose.push(record.end());
            }
            if needs_type {
                ty.push(record.ty());
            }
            if needs_source {
                source.push(record.source());
            }
            if needs_score {
                scores.push(record.score());
            }
            if needs_strand {
                strand.push(record.strand());
            }
            if needs_phase {
                phase.push(record.phase().map(|p| p as u32));
            }

            // CONDITIONAL PARSING: Only parse attributes when actually needed
            // Logic:
            // - SELECT * or SELECT attributes -> !unnest_enable (parse all attributes)
            // - SELECT gene_id, transcript_id -> unnest_enable && !attribute_builders.0.is_empty() (parse specific)
            // - SELECT chrom, start, end -> unnest_enable && attribute_builders.0.is_empty() (skip parsing)
            let needs_attributes = if !unnest_enable {
                // Always parse when using nested attributes structure (SELECT * or SELECT attributes)
                true
            } else {
                // Only parse when specific attribute fields are requested
                !attribute_builders.0.is_empty()
            };
            if needs_attributes {
                if unnest_enable && !attribute_builders.0.is_empty() {
                    // Parse attributes from the record's attributes string
                    let attributes_str = record.attributes_string();
                    let attributes_map = parse_gff_attributes(&attributes_str);
                    load_attributes_unnest_from_map(&attributes_map, &mut attribute_builders, projection.clone())?;
                } else if !unnest_enable {
                    // Parse attributes into nested structure
                    let attributes_str = record.attributes_string();
                    let attributes_map = parse_gff_attributes(&attributes_str);
                    load_attributes_from_map(&attributes_map, &mut builder)?;
                }
            } else {
                // OPTIMIZATION: Skip attribute parsing entirely when not needed
                // This saves 3.1M expensive parsing operations!
                if unnest_enable {
                    // Still need to maintain builder consistency - append nulls
                    for builder in &mut attribute_builders.2 {
                        builder.append_null()?;
                    }
                } else {
                    // Append null for nested structure
                    builder[0].append_null()?;
                }
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
                    needs_chrom,
                    needs_start,
                    needs_end,
                    needs_type,
                    needs_source,
                    needs_score,
                    needs_strand,
                    needs_phase,
                    batch_size,
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
        let remaining_records = record_num % batch_size;
        if remaining_records != 0 {
            let batch = build_record_batch_optimized(
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
                needs_chrom,
                needs_start,
                needs_end,
                needs_type,
                needs_source,
                needs_score,
                needs_strand,
                needs_phase,
                remaining_records,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

// NOTE: Removed build_record_batch function - replaced with optimized build_record_batch_optimized

fn build_record_batch_optimized(
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
    needs_chrom: bool,
    needs_start: bool,
    needs_end: bool,
    needs_type: bool,
    needs_source: bool,
    needs_score: bool,
    needs_strand: bool,
    needs_phase: bool,
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

    let ty_array = if needs_type {
        Arc::new(StringArray::from(ty.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let source_array = if needs_source {
        Arc::new(StringArray::from(source.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let score_array = if needs_score {
        Arc::new({
            let mut builder = Float32Builder::new();
            for s in score {
                builder.append_option(*s);
            }
            builder.finish()
        }) as Arc<dyn Array>
    } else {
        Arc::new({
            let mut builder = Float32Builder::new();
            for _ in 0..record_count {
                builder.append_null();
            }
            builder.finish()
        }) as Arc<dyn Array>
    };

    let strand_array = if needs_strand {
        Arc::new(StringArray::from(strand.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let phase_array = if needs_phase {
        Arc::new({
            let mut builder = UInt32Builder::new();
            for s in phase {
                builder.append_option(*s);
            }
            builder.finish()
        }) as Arc<dyn Array>
    } else {
        Arc::new({
            let mut builder = UInt32Builder::new();
            for _ in 0..record_count {
                builder.append_null();
            }
            builder.finish()
        }) as Arc<dyn Array>
    };

    let arrays = match projection {
        None => {
            // Check if schema is empty (for COUNT(*) queries)
            if schema.fields().is_empty() {
                Vec::new()
            } else {
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
                if let Some(attr_arrays) = attributes {
                    arrays.append(&mut attr_arrays.clone());
                }
                arrays
            }
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(proj_ids.len());
            if proj_ids.is_empty() {
                debug!("Empty projection - will create null array after this match");
                // Arrays will be empty, will be handled after the match block
            } else {
                for i in proj_ids {
                    match i {
                        0 => arrays.push(chrom_array.clone()),
                        1 => arrays.push(pos_start_array.clone()),
                        2 => arrays.push(pos_end_array.clone()),
                        3 => arrays.push(ty_array.clone()),
                        4 => arrays.push(source_array.clone()),
                        5 => arrays.push(score_array.clone()),
                        6 => arrays.push(strand_array.clone()),
                        7 => arrays.push(phase_array.clone()),
                        _ => {
                            if let Some(attr_arrays) = attributes {
                                if i >= 8 && (i - 8) < attr_arrays.len() {
                                    arrays.push(attr_arrays[i - 8].clone());
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

    if arrays.is_empty() {
        // For COUNT(*) queries, create an empty RecordBatch with the correct row count
        // Arrow allows this via RecordBatch::try_new with an empty schema and empty arrays
        RecordBatch::try_new_with_options(
            schema,
            arrays,
            &datafusion::arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(record_count)),
        )
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Error creating empty GFF batch for COUNT(*): {:?}",
                e
            ))
        })
    } else {
        RecordBatch::try_new(schema, arrays).map_err(|e| {
            DataFusionError::Execution(format!("Error creating optimized GFF batch: {:?}", e))
        })
    }
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
