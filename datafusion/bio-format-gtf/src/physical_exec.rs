use crate::storage::{GtfLocalReader, GtfRecordTrait};
use async_stream::try_stream;
use datafusion::arrow::array::{
    Array, Float32Builder, NullArray, RecordBatch, StringArray, UInt32Array, UInt32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::table_utils::{Attribute, OptionalField, builders_to_arrays};
use futures_util::TryStreamExt;
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct GtfExec {
    pub(crate) file_path: String,
    pub(crate) attr_fields: Option<Vec<String>>,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) filters: Vec<Expr>,
    pub(crate) cache: PlanProperties,
    pub(crate) coordinate_system_zero_based: bool,
}

impl Debug for GtfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GtfExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for GtfExec {
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
        write!(f, "GtfExec: projection=[{proj_str}]")
    }
}

impl ExecutionPlan for GtfExec {
    fn name(&self) -> &str {
        "GtfExec"
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
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();

        let fut = get_local_gtf(
            self.file_path.clone(),
            self.attr_fields.clone(),
            schema.clone(),
            batch_size,
            self.projection.clone(),
            self.filters.clone(),
            self.coordinate_system_zero_based,
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

/// Parse GTF attributes string into Attribute structs
///
/// GTF format: `key "value"; key "value";`
/// Some values may be unquoted (e.g., `level 2`).
/// Duplicate keys are supported (e.g., multiple `tag` entries).
fn parse_gtf_attributes_to_vec(attributes_str: &str) -> Vec<Attribute> {
    if attributes_str.is_empty() || attributes_str == "." {
        return Vec::new();
    }

    let estimated_pairs = attributes_str.matches(';').count() + 1;
    let mut attributes = Vec::with_capacity(estimated_pairs);

    for pair in attributes_str.split(';') {
        let trimmed = pair.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(space_pos) = trimmed.find(' ') {
            let key = &trimmed[..space_pos];
            let value = trimmed[space_pos + 1..].trim();

            let unquoted = if value.starts_with('"') && value.ends_with('"') {
                value[1..value.len() - 1].to_string()
            } else {
                value.to_string()
            };

            attributes.push(Attribute {
                tag: key.to_string(),
                value: Some(unquoted),
            });
        }
    }

    attributes
}

/// Load GTF attributes for specific attribute columns (unnested)
fn load_attributes_unnest_from_string(
    attributes_str: &str,
    attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
    projection: Option<Vec<usize>>,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let mut attributes_map = HashMap::new();

    if !attributes_str.is_empty() && attributes_str != "." {
        for pair in attributes_str.split(';') {
            let trimmed = pair.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Some(space_pos) = trimmed.find(' ') {
                let key = &trimmed[..space_pos];

                if attribute_builders.0.contains(&key.to_string()) {
                    let value = trimmed[space_pos + 1..].trim();
                    let unquoted = if value.starts_with('"') && value.ends_with('"') {
                        value[1..value.len() - 1].to_string()
                    } else {
                        value.to_string()
                    };
                    // For duplicate keys, keep the first value (consistent with attribute extraction)
                    attributes_map.entry(key.to_string()).or_insert(unquoted);
                }
            }
        }
    }

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

fn load_attributes_from_vec(
    attributes: Vec<Attribute>,
    builder: &mut [OptionalField],
) -> Result<(), datafusion::arrow::error::ArrowError> {
    builder[0].append_array_struct(attributes)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn get_local_gtf(
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    coordinate_system_zero_based: bool,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let needs_chrom = projection.as_ref().is_none_or(|proj| proj.contains(&0));
    let needs_start = projection.as_ref().is_none_or(|proj| proj.contains(&1));
    let needs_end = projection.as_ref().is_none_or(|proj| proj.contains(&2));
    let needs_type = projection.as_ref().is_none_or(|proj| proj.contains(&3));
    let needs_source = projection.as_ref().is_none_or(|proj| proj.contains(&4));
    let needs_score = projection.as_ref().is_none_or(|proj| proj.contains(&5));
    let needs_strand = projection.as_ref().is_none_or(|proj| proj.contains(&6));
    let needs_phase = projection.as_ref().is_none_or(|proj| proj.contains(&7));

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
    let mut ty: Vec<String> = if needs_type {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut source: Vec<String> = if needs_source {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut scores: Vec<Option<f32>> = if needs_score {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut strand: Vec<String> = if needs_strand {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut phase: Vec<Option<u32>> = if needs_phase {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };

    let reader = GtfLocalReader::new(&file_path).map_err(|e| {
        DataFusionError::Execution(format!("Failed to open GTF file '{file_path}': {e}"))
    })?;
    let sync_iter = reader.into_sync_iterator();

    let mut attribute_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());

    let unnest_enable = match &attr_fields {
        Some(attrs) => {
            set_attribute_builders(batch_size, attrs, &mut attribute_builders);
            true
        }
        None => false,
    };

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
        for result in sync_iter {
            let record = result?;

            let attributes_str = record.attributes_string();
            if !crate::filter_utils::evaluate_filters_against_record(&record, &filters, &attributes_str) {
                continue;
            }

            if needs_chrom {
                chroms.push(record.reference_sequence_name());
            }
            if needs_start {
                let start_pos = record.start();
                poss.push(if coordinate_system_zero_based { start_pos - 1 } else { start_pos });
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

            let needs_attributes = if !unnest_enable {
                true
            } else {
                !attribute_builders.0.is_empty()
            };
            if needs_attributes {
                if unnest_enable && !attribute_builders.0.is_empty() {
                    load_attributes_unnest_from_string(&attributes_str, &mut attribute_builders, projection.clone())?;
                } else if !unnest_enable {
                    let attributes = parse_gtf_attributes_to_vec(&attributes_str);
                    load_attributes_from_vec(attributes, &mut builder)?;
                }
            } else if unnest_enable {
                for b in &mut attribute_builders.2 {
                    b.append_null()?;
                }
            } else {
                builder[0].append_null()?;
            }

            record_num += 1;
            if record_num % batch_size == 0 {
                debug!("GTF record number: {record_num}");
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema),
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
                        })),
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
                yield batch;
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
        let remaining_records = record_num % batch_size;
        if remaining_records != 0 {
            let batch = build_record_batch_optimized(
                Arc::clone(&schema),
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
                    })),
                projection.clone(),
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

#[allow(clippy::too_many_arguments)]
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
            if !proj_ids.is_empty() {
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
        RecordBatch::try_new_with_options(
            schema,
            arrays,
            &datafusion::arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(record_count)),
        )
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Error creating empty GTF batch for COUNT(*): {e:?}"
            ))
        })
    } else {
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| DataFusionError::Execution(format!("Error creating GTF batch: {e:?}")))
    }
}
