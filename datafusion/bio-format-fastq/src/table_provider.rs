use crate::physical_exec::FastqExec;
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

/// Byte range specification for file reading
#[derive(Clone, Debug, PartialEq)]
pub struct FastqByteRange {
    pub start: u64,
    pub end: u64,
}

fn determine_schema() -> datafusion::common::Result<SchemaRef> {
    let fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
    ];
    let schema = Schema::new(fields);
    debug!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

#[derive(Clone, Debug)]
pub struct FastqTableProvider {
    file_path: String,
    schema: SchemaRef,
    thread_num: Option<usize>,
    byte_range: Option<FastqByteRange>,
    object_storage_options: Option<ObjectStorageOptions>,
}

impl FastqTableProvider {
    pub fn new(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema()?;
        Ok(Self {
            file_path,
            schema,
            thread_num,
            byte_range: None,
            object_storage_options,
        })
    }

    /// Create FastqTableProvider that reads only a specific byte range
    pub fn new_with_range(
        file_path: String,
        byte_range: Option<FastqByteRange>, // None = read entire file
        thread_num: Option<usize>,          // For distributed use, should be None (single-threaded)
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema()?;
        Ok(Self {
            file_path,
            schema,
            thread_num,
            byte_range,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for FastqTableProvider {
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
        debug!("FastqTableProvider::scan");

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

        Ok(Arc::new(FastqExec {
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
            byte_range: self.byte_range.clone(),
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
