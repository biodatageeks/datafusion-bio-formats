use crate::physical_exec::GffExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use log::debug;
use std::any::Any;
use std::sync::Arc;

/// Constructs schema on-demand based on requested attribute fields from Python layer
/// This eliminates the need for file scanning and supports two distinct modes:
///
/// Mode 1 (Default): No specific attributes requested -> nested attributes structure
/// Mode 2 (Projection): Specific attributes requested -> flattened individual columns
fn determine_schema_on_demand(
    attr_fields: Option<Vec<String>>,
) -> datafusion::common::Result<SchemaRef> {
    // Always include 8 static GFF fields
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::UInt32, true),
    ];

    match attr_fields {
        None => {
            // Mode 1: Default - return nested attributes for maximum flexibility
            fields.push(Field::new(
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
            ));
            debug!(
                "GFF Schema Mode 1 (Default): 8 static fields + nested attributes = 9 columns total"
            );
        }
        Some(attrs) => {
            // Mode 2: Projection - add requested attributes as individual columns
            // All GFF attributes are treated as nullable strings for simplicity
            for attr_name in &attrs {
                fields.push(Field::new(
                    attr_name,
                    DataType::Utf8, // All GFF attributes are strings
                    true,           // Attributes can be null
                ));
            }
            debug!(
                "GFF Schema Mode 2 (Projection): 8 static fields + {} attribute fields = {} columns total",
                attrs.len(),
                8 + attrs.len()
            );
        }
    }

    let schema = Schema::new(fields);
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
    /// Creates a new GFF table provider with projection-driven schema construction.
    ///
    /// The schema is built immediately based on the attr_fields parameter from Python:
    /// - None: Creates default schema with nested attributes (9 columns)
    /// - Some(attrs): Creates projection schema with flattened attributes (8 + N columns)
    pub fn new(
        file_path: String,
        attr_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        // NEW: Schema construction based on Python-provided attribute fields
        let schema = determine_schema_on_demand(attr_fields.clone())?;

        debug!(
            "GffTableProvider::new - constructed schema for file: {}",
            file_path
        );

        Ok(Self {
            file_path,
            attr_fields, // Store original Python-provided fields for execution
            schema,      // Pre-constructed based on request
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
        debug!("GffTableProvider::scan with projection: {:?}", projection);

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

        // Schema is already constructed correctly in constructor based on Python categorization
        // Just apply DataFusion's standard projection logic
        let projected_schema = project_schema(&self.schema, projection);
        debug!(
            "GFF projected schema has {} columns",
            projected_schema.fields().len()
        );

        Ok(Arc::new(GffExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            attr_fields: self.attr_fields.clone(), // Pass original Python-provided fields to executor
            schema: projected_schema.clone(),
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
