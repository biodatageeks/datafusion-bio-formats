use crate::physical_exec::CramExec;
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
use log::debug;
use std::any::Any;
use std::sync::Arc;

fn determine_schema() -> datafusion::common::Result<SchemaRef> {
    let fields = vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("chrom", DataType::Utf8, true),
        Field::new("start", DataType::UInt32, true),
        Field::new("end", DataType::UInt32, true),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, true),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
    ];
    let schema = Schema::new(fields);
    debug!("CRAM Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

#[derive(Clone, Debug)]
pub struct CramTableProvider {
    file_path: String,
    schema: SchemaRef,
    reference_path: Option<String>,
    object_storage_options: Option<ObjectStorageOptions>,
}

impl CramTableProvider {
    pub fn new(
        file_path: String,
        reference_path: Option<String>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema()?;
        Ok(Self {
            file_path,
            schema,
            reference_path,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for CramTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("CramTableProvider::scan");

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

        Ok(Arc::new(CramExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            limit,
            reference_path: self.reference_path.clone(),
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
