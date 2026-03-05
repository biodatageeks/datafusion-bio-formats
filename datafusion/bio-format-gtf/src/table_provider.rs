use crate::filter_utils::can_push_down_filter;
use crate::physical_exec::GtfExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Constructs schema on-demand based on requested attribute fields
fn determine_schema_on_demand(
    attr_fields: Option<Vec<String>>,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<SchemaRef> {
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
            fields.push(Field::new(
                "attributes",
                DataType::List(FieldRef::from(Box::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    true,
                )))),
                true,
            ));
            debug!(
                "GTF Schema Mode 1 (Default): 8 static fields + nested attributes = 9 columns total"
            );
        }
        Some(attrs) => {
            for attr_name in &attrs {
                fields.push(Field::new(attr_name, DataType::Utf8, true));
            }
            debug!(
                "GTF Schema Mode 2 (Projection): 8 static fields + {} attribute fields = {} columns total",
                attrs.len(),
                8 + attrs.len()
            );
        }
    }

    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    let schema = Schema::new_with_metadata(fields, metadata);
    Ok(Arc::new(schema))
}

/// Apache DataFusion table provider for GTF bioinformatics files
///
/// Implements the TableProvider trait to enable seamless SQL queries on GTF files.
/// Features include:
/// - Projection-driven schema construction for attribute columns
/// - Filter pushdown support for efficient predicate evaluation
/// - Compressed and uncompressed file handling
#[derive(Clone, Debug)]
pub struct GtfTableProvider {
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    coordinate_system_zero_based: bool,
}

impl GtfTableProvider {
    /// Creates a new GTF table provider with projection-driven schema construction.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the GTF file
    /// * `attr_fields` - Optional list of attribute fields to include
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
    pub fn new(
        file_path: String,
        attr_fields: Option<Vec<String>>,
        coordinate_system_zero_based: bool,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema_on_demand(attr_fields.clone(), coordinate_system_zero_based)?;

        debug!("GtfTableProvider::new - constructed schema for file: {file_path}");

        Ok(Self {
            file_path,
            attr_fields,
            schema,
            coordinate_system_zero_based,
        })
    }
}

#[async_trait]
impl TableProvider for GtfTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        let pushdown_support = filters
            .iter()
            .map(|expr| {
                if can_push_down_filter(expr, &self.schema) {
                    debug!("GTF filter can be pushed down: {expr:?}");
                    TableProviderFilterPushDown::Inexact
                } else {
                    debug!("GTF filter cannot be pushed down: {expr:?}");
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(pushdown_support)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    let empty_fields: Vec<Field> = vec![];
                    Arc::new(Schema::new_with_metadata(
                        empty_fields,
                        schema.metadata().clone(),
                    ))
                }
                Some(indices) => {
                    let projected_fields: Vec<Field> =
                        indices.iter().map(|&i| schema.field(i).clone()).collect();
                    Arc::new(Schema::new_with_metadata(
                        projected_fields,
                        schema.metadata().clone(),
                    ))
                }
                None => schema.clone(),
            }
        }

        let projected_schema = project_schema(&self.schema, projection);

        let pushable_filters: Vec<Expr> = filters
            .iter()
            .filter(|expr| can_push_down_filter(expr, &self.schema))
            .cloned()
            .collect();

        Ok(Arc::new(GtfExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            attr_fields: self.attr_fields.clone(),
            schema: projected_schema,
            projection: projection.cloned(),
            filters: pushable_filters,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
        }))
    }
}
