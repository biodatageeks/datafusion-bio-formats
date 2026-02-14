use crate::physical_exec::FastaExec;
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

/// Determines the Arrow schema for FASTA file records.
///
/// Returns a schema with three fields:
/// - `name` (Utf8, non-null): Sequence identifier
/// - `description` (Utf8, nullable): Sequence description
/// - `sequence` (Utf8, non-null): The actual sequence data
fn determine_schema() -> datafusion::common::Result<SchemaRef> {
    let fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("sequence", DataType::Utf8, false),
    ];
    let schema = Schema::new(fields);
    debug!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

/// DataFusion table provider for FASTA files.
///
/// This struct implements the `TableProvider` trait to enable querying FASTA files
/// using DataFusion SQL queries. It supports local files and cloud storage with
/// automatic compression detection.
///
/// # Example
///
/// ```rust,no_run
/// use datafusion::prelude::*;
/// use datafusion_bio_format_fasta::table_provider::FastaTableProvider;
/// use std::sync::Arc;
///
/// # async fn example() -> datafusion::error::Result<()> {
/// let ctx = SessionContext::new();
/// let table = FastaTableProvider::new("sequences.fasta".to_string(), None)?;
/// ctx.register_table("fasta", Arc::new(table))?;
///
/// let df = ctx.sql("SELECT name, sequence FROM fasta WHERE length(sequence) > 100").await?;
/// df.show().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FastaTableProvider {
    /// Path to the FASTA file
    file_path: String,
    /// Arrow schema describing FASTA records
    schema: SchemaRef,
    /// Cloud storage configuration
    object_storage_options: Option<ObjectStorageOptions>,
}

impl FastaTableProvider {
    /// Creates a new FASTA table provider.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the FASTA file (local or cloud storage URI)
    /// * `object_storage_options` - Optional cloud storage configuration
    ///
    /// # Returns
    ///
    /// A new `FastaTableProvider` instance or an error if schema determination fails.
    pub fn new(
        file_path: String,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema()?;
        Ok(Self {
            file_path,
            schema,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for FastaTableProvider {
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
        debug!("FastaTableProvider::scan");

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

        Ok(Arc::new(FastaExec {
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
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
