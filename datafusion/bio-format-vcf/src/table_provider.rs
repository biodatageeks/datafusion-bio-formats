use crate::physical_exec::VcfExec;
use crate::storage::get_header;
use async_trait::async_trait;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use futures::executor::block_on;
use log::debug;
use noodles_vcf::header::Formats;
use noodles_vcf::header::Infos;
use noodles_vcf::header::record::value::map::format::Type as FormatType;
use noodles_vcf::header::record::value::map::info::{Number, Type as InfoType};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

async fn determine_schema_from_header(
    file_path: &str,
    info_fields: &Option<Vec<String>>,
    format_fields: &Option<Vec<String>>,
    object_storage_options: &Option<ObjectStorageOptions>,
) -> datafusion::common::Result<SchemaRef> {
    let header = get_header(file_path.to_string(), object_storage_options.clone()).await?;
    let header_infos = header.infos();
    let header_formats = header.formats();

    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
    ];

    match info_fields {
        Some(infos) => {
            for tag in infos {
                let dtype = info_to_arrow_type(&header_infos, &tag);
                let info = header_infos.get(tag.as_str()).unwrap();
                let nullable = is_nullable(&info.ty());
                fields.push(Field::new(tag, dtype, nullable)); // Convert to lowercase for schema (backward compatibility)
            }
        }
        _ => {}
    }

    match format_fields {
        Some(formats) => {
            for tag in formats {
                let dtype = format_to_arrow_type(&header_formats, &tag);
                fields.push(Field::new(format!("format_{}", tag), dtype, true));
            }
        }
        _ => {}
    }

    let schema = Schema::new(fields);
    // println!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

async fn determine_full_schema_from_header(
    file_path: &str,
    object_storage_options: &Option<ObjectStorageOptions>,
) -> datafusion::common::Result<(SchemaRef, Vec<String>, Vec<String>)> {
    let header = get_header(file_path.to_string(), object_storage_options.clone()).await?;
    let header_infos = header.infos();
    let header_formats = header.formats();

    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
    ];

    let mut all_info_fields = Vec::new();
    for (tag, info) in header_infos.iter() {
        let dtype = info_to_arrow_type(&header_infos, tag);
        let nullable = is_nullable(&info.ty());
        fields.push(Field::new(tag, dtype, nullable)); // Convert to lowercase for schema (backward compatibility)
        all_info_fields.push(tag.to_string()); // Keep original case for VCF processing
    }

    let mut all_format_fields = Vec::new();
    for (tag, _format) in header_formats.iter() {
        let dtype = format_to_arrow_type(&header_formats, tag);
        fields.push(Field::new(format!("format_{}", tag), dtype, true));
        all_format_fields.push(tag.to_string());
    }

    let schema = Schema::new(fields);
    Ok((Arc::new(schema), all_info_fields, all_format_fields))
}

fn extract_needed_fields_from_projection(
    projection: Option<&Vec<usize>>,
    full_schema: &SchemaRef,
) -> (Option<Vec<String>>, Option<Vec<String>>) {
    let projection = match projection {
        Some(proj) => proj,
        None => return (None, None), // No projection means all fields needed
    };

    let mut needed_info_fields = Vec::new();
    let mut needed_format_fields = Vec::new();

    for &col_idx in projection {
        if col_idx >= 8 {
            // Info and format fields start at index 8
            let field_name = full_schema.field(col_idx).name();
            if field_name.starts_with("format_") {
                needed_format_fields.push(field_name.strip_prefix("format_").unwrap().to_string());
            } else {
                // Field names are lowercase in schema, but VCF processing needs uppercase
                needed_info_fields.push(field_name.to_string());
            }
        }
    }

    let info_fields = if needed_info_fields.is_empty() {
        None
    } else {
        Some(needed_info_fields)
    };

    let format_fields = if needed_format_fields.is_empty() {
        None
    } else {
        Some(needed_format_fields)
    };

    (info_fields, format_fields)
}

fn is_nullable(ty: &InfoType) -> bool {
    !matches!(ty, InfoType::Flag)
}

fn format_to_arrow_type(formats: &Formats, field: &str) -> DataType {
    let format = formats.get(field).unwrap();
    match format.ty() {
        FormatType::Integer => DataType::Int32,
        FormatType::Float => DataType::Float32,
        FormatType::Character => DataType::Utf8,
        FormatType::String => DataType::Utf8,
    }
}

#[derive(Clone, Debug)]
pub struct VcfTableProvider {
    file_path: String,
    requested_info_fields: Option<Vec<String>>,
    requested_format_fields: Option<Vec<String>>,
    full_schema: Option<SchemaRef>,
    all_info_fields: Option<Vec<String>>,
    all_format_fields: Option<Vec<String>>,
    thread_num: Option<usize>,
    object_storage_options: Option<ObjectStorageOptions>,
}

impl VcfTableProvider {
    pub fn new(
        file_path: String,
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            file_path,
            requested_info_fields: info_fields,
            requested_format_fields: format_fields,
            full_schema: None,
            all_info_fields: None,
            all_format_fields: None,
            thread_num,
            object_storage_options,
        })
    }

    async fn ensure_full_schema(&mut self) -> datafusion::common::Result<()> {
        if self.full_schema.is_none() {
            // Use the requested fields to create the schema, but discover all available fields for optimization
            let (_schema, all_info_fields, all_format_fields) =
                determine_full_schema_from_header(&self.file_path, &self.object_storage_options)
                    .await?;

            // Create schema with only the requested fields (to maintain backward compatibility)
            let requested_schema = determine_schema_from_header(
                &self.file_path,
                &self.requested_info_fields,
                &self.requested_format_fields,
                &self.object_storage_options,
            )
            .await?;

            self.full_schema = Some(requested_schema);
            self.all_info_fields = Some(all_info_fields);
            self.all_format_fields = Some(all_format_fields);
        }
        Ok(())
    }
}

#[async_trait]
impl TableProvider for VcfTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
        // todo!()
    }

    fn schema(&self) -> SchemaRef {
        // If we have the full schema, use it
        if let Some(ref schema) = self.full_schema {
            return schema.clone();
        }

        // If no full schema yet, we need to determine it lazily
        // This is a fallback - ideally scan() should be called first
        let mut provider_clone = self.clone();
        let schema = block_on(async {
            provider_clone.ensure_full_schema().await?;
            Ok::<SchemaRef, datafusion::common::DataFusionError>(
                provider_clone.full_schema.unwrap(),
            )
        })
        .unwrap_or_else(|_| {
            // If we can't get the schema, return a minimal core schema
            Arc::new(Schema::new(vec![
                Field::new("chrom", DataType::Utf8, false),
                Field::new("start", DataType::UInt32, false),
                Field::new("end", DataType::UInt32, false),
                Field::new("id", DataType::Utf8, true),
                Field::new("ref", DataType::Utf8, false),
                Field::new("alt", DataType::Utf8, false),
                Field::new("qual", DataType::Float64, true),
                Field::new("filter", DataType::Utf8, true),
            ]))
        });
        schema
    }

    fn table_type(&self) -> TableType {
        TableType::Base
        // todo!()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("VcfTableProvider::scan with projection: {:?}", projection);

        // Make a mutable clone to ensure full schema is available
        let mut provider_clone = self.clone();
        provider_clone.ensure_full_schema().await?;
        let full_schema = provider_clone.full_schema.as_ref().unwrap();

        // Extract only the info and format fields that are actually needed based on projection
        let (optimized_info_fields, optimized_format_fields) =
            extract_needed_fields_from_projection(projection, full_schema);

        debug!("Optimized info fields: {:?}", optimized_info_fields);
        debug!("Optimized format fields: {:?}", optimized_format_fields);

        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    // For empty projections (COUNT(*)), return an empty schema
                    Arc::new(Schema::empty())
                }
                Some(indices) => {
                    let projected_fields: Vec<Field> =
                        indices.iter().map(|&i| schema.field(i).clone()).collect();
                    Arc::new(Schema::new(projected_fields))
                }
                None => schema.clone(),
            }
        }

        let projected_schema = project_schema(full_schema, projection);

        Ok(Arc::new(VcfExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: projected_schema.clone(),
            info_fields: optimized_info_fields, // Only needed fields!
            format_fields: optimized_format_fields, // Only needed fields!
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}

pub fn info_to_arrow_type(infos: &Infos, field: &str) -> DataType {
    match infos.get(field) {
        Some(t) => {
            let inner = match t.ty() {
                InfoType::Integer => DataType::Int32,
                InfoType::String | InfoType::Character => DataType::Utf8,
                InfoType::Float => DataType::Float32,
                InfoType::Flag => DataType::Boolean,
            };

            match t.number() {
                Number::Count(0) | Number::Count(1) => inner,
                Number::Count(_)
                | Number::Unknown
                | Number::AlternateBases
                | Number::ReferenceAlternateBases
                | Number::Samples => DataType::List(Arc::new(Field::new("item", inner, true))),
            }
        }
        None => {
            log::warn!(
                "VCF tag '{}' not found in header; defaulting to Utf8",
                field
            );
            DataType::Utf8
        }
    }
}
