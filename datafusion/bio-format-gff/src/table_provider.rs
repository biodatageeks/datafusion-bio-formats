use crate::physical_exec::GffExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use log::debug;
use std::any::Any;
use std::sync::Arc;

pub fn get_attribute_names_and_types(attributes: Vec<String>) -> (Vec<String>, Vec<DataType>) {
    let mut attribute_names = Vec::new();
    let mut attribute_types = Vec::new();

    for attr in attributes {
        match attr.split_once(':') {
            Some((n, t)) => {
                if t.to_lowercase() == "string" {
                    attribute_types.push(DataType::Utf8);
                } else if t.to_lowercase() == "array" {
                    attribute_types.push(DataType::List(Arc::new(Field::new(
                        n.to_string(),
                        DataType::Utf8,
                        true,
                    ))));
                } else {
                    attribute_types.push(DataType::Utf8); // Default to Utf8 if type is unknown
                }
                attribute_names.push(n.to_string());
            }
            None => {
                attribute_types.push(DataType::Utf8);
                attribute_names.push(attr.to_string());
            }
        };
    }
    (attribute_names, attribute_types)
}
fn determine_schema(attr_fields: Option<Vec<String>>) -> datafusion::common::Result<SchemaRef> {
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::UInt32, true), //FIXME:: can be downcasted to 8
    ];
    match attr_fields {
        None => fields.push(Field::new(
            "attributes",
            DataType::List(FieldRef::from(Box::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("tag", DataType::Utf8, false), // tag must be non-null
                    Field::new("value", DataType::Utf8, true), // value may be null
                ])),
                true,
            )))),
            true,
        )),
        _ => {
            let attributes = get_attribute_names_and_types(attr_fields.unwrap());
            for (name, dtype) in attributes.0.iter().zip(attributes.1.iter()) {
                fields.push(Field::new(name, dtype.clone(), true));
            }
        }
    }

    let schema = Schema::new(fields);
    debug!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

#[derive(Clone, Debug)]
pub struct GffTableProvider {
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    thread_num: Option<usize>,
    object_storage_options: Option<ObjectStorageOptions>,
}

impl GffTableProvider {
    pub fn new(
        file_path: String,
        attr_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema(attr_fields.clone())?;
        Ok(Self {
            file_path,
            attr_fields,
            schema,
            thread_num,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for GffTableProvider {
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
        debug!("GffTableProvider::scan");

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

        Ok(Arc::new(GffExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            file_path: self.file_path.clone(),
            attr_fields: self.attr_fields.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
