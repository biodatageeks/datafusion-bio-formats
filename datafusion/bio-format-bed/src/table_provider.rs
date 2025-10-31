use crate::physical_exec::BedExec;
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

/// Enumeration of supported BED format variants based on number of columns
///
/// BED (Browser Extensible Data) files support different column counts:
/// - BED3: 3 columns (chrom, start, end)
/// - BED4: 4 columns (chrom, start, end, name)
/// - BED5: 5 columns (chrom, start, end, name, score)
/// - BED6: 6 columns (chrom, start, end, name, score, strand)
#[derive(Debug, Clone)]
pub enum BEDFields {
    /// 3-column BED format: chrom, start, end
    BED3,
    /// 4-column BED format: chrom, start, end, name
    BED4,
    /// 5-column BED format: chrom, start, end, name, score
    BED5,
    /// 6-column BED format: chrom, start, end, name, score, strand
    BED6,
}

/// Determines the schema for BED table data
///
/// Returns a schema with the following fields:
/// - `chrom` (Utf8, not nullable): Chromosome name
/// - `start` (UInt32, not nullable): Start position (0-based)
/// - `end` (UInt32, not nullable): End position (exclusive)
/// - `name` (Utf8, nullable): Feature name
fn determine_schema() -> datafusion::common::Result<SchemaRef> {
    let fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, true),
    ];
    let schema = Schema::new(fields);
    debug!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

/// A DataFusion TableProvider for reading BED files
///
/// This struct implements the [`TableProvider`] trait to enable SQL queries over BED files.
/// It supports both local and remote (cloud) storage backends, with configurable
/// parallelism and compression handling.
///
/// # Example
///
/// ```rust,no_run
/// use datafusion_bio_format_bed::table_provider::{BedTableProvider, BEDFields};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let table = BedTableProvider::new(
///     "data/genes.bed".to_string(),
///     BEDFields::BED4,
///     Some(4),  // Use 4 threads for parallel reading
///     None,     // No cloud storage options
/// )?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct BedTableProvider {
    /// Path to the BED file (local or remote)
    file_path: String,
    /// BED format variant specifying column count
    bed_fields: BEDFields,
    /// Arrow schema for the table
    schema: SchemaRef,
    /// Optional number of threads for parallel reading
    thread_num: Option<usize>,
    /// Optional cloud storage configuration
    object_storage_options: Option<ObjectStorageOptions>,
}

impl BedTableProvider {
    /// Creates a new BED table provider
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the BED file (local filesystem or cloud storage URL)
    /// * `bed_fields` - BED format variant (BED3, BED4, BED5, BED6)
    /// * `thread_num` - Optional number of threads for parallel BGZF decompression
    /// * `object_storage_options` - Optional cloud storage configuration for remote files
    ///
    /// # Returns
    ///
    /// Returns a new `BedTableProvider` or an error if schema initialization fails
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be created
    pub fn new(
        file_path: String,
        bed_fields: BEDFields,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema()?;
        Ok(Self {
            file_path,
            bed_fields,
            schema,
            thread_num,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for BedTableProvider {
    /// Returns `self` as `Any` for dynamic type casting
    fn as_any(&self) -> &dyn Any {
        self
        // todo!()
    }

    /// Returns the schema of the table
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the table type (always Base for BED files)
    fn table_type(&self) -> TableType {
        TableType::Base
        // todo!()
    }

    /// Creates an execution plan for scanning the BED file
    ///
    /// # Arguments
    ///
    /// * `_state` - Session state (unused)
    /// * `projection` - Optional column indices to project
    /// * `_filters` - Filter expressions (not currently applied)
    /// * `limit` - Optional row limit
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("BedTableProvider::scan");

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

        Ok(Arc::new(BedExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            bed_fields: self.bed_fields.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
