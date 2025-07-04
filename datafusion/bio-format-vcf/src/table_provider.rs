use crate::physical_exec::VcfExec;
use crate::storage::get_header;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use futures::executor::block_on;
use log::debug;
use noodles::vcf::header::Formats;
use noodles::vcf::header::Infos;
use noodles::vcf::header::record::value::map::format::Type as FormatType;
use noodles::vcf::header::record::value::map::info::{Number, Type as InfoType};
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
                fields.push(Field::new(tag.to_lowercase(), dtype, nullable));
            }
        }
        _ => {}
    }

    match format_fields {
        Some(formats) => {
            for tag in formats {
                let dtype = format_to_arrow_type(&header_formats, &tag);
                fields.push(Field::new(
                    format!("format_{}", tag.to_lowercase()),
                    dtype,
                    true,
                ));
            }
        }
        _ => {}
    }

    let schema = Schema::new(fields);
    // println!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
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
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    schema: SchemaRef,
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
        let schema = block_on(determine_schema_from_header(
            &file_path,
            &info_fields,
            &format_fields,
            &object_storage_options,
        ))?;
        Ok(Self {
            file_path,
            info_fields,
            format_fields,
            schema,
            thread_num,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for VcfTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
        // todo!()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        debug!("VcfTableProvider::scan");

        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    Arc::new(Schema::new(vec![Field::new("dummy", DataType::Null, true)]))
                }
                Some(indices) => {
                    let projected_fields: Vec<Field> =
                        indices.iter().map(|&i| schema.field(i).clone()).collect();
                    Arc::new(Schema::new(projected_fields))
                }
                None => schema.clone(),
            }
        }

        let schema = project_schema(&self.schema, projection);

        Ok(Arc::new(VcfExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            info_fields: self.info_fields.clone(),
            format_fields: self.format_fields.clone(),
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
