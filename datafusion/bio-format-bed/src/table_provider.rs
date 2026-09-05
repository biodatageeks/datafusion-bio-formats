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
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Selects the output columns of a BED scan.
///
/// All modes require the three core fields; absent optional fields are null.
/// Later columns are accepted but not interpreted. The output modes are:
/// - BED3: 3 columns (chrom, start, end)
/// - BED4: 4 columns (chrom, start, end, name)
/// - BED5: 5 columns (chrom, start, end, name, score)
/// - BED6: 6 columns (chrom, start, end, name, score, strand)
#[derive(Debug, Clone, Copy)]
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

impl BEDFields {
    pub(crate) fn count(self) -> usize {
        match self {
            Self::BED3 => 3,
            Self::BED4 => 4,
            Self::BED5 => 5,
            Self::BED6 => 6,
        }
    }
}

/// Determines the schema for BED table data
///
/// Returns a schema with the following fields:
/// - `chrom` (Utf8, not nullable): Chromosome name
/// - `start` (UInt32, not nullable): Start position (0-based)
/// - `end` (UInt32, not nullable): End position (exclusive)
/// - `name` (Utf8, nullable): Feature name (BED4+)
/// - `score` (UInt16, nullable): Score (BED5+)
/// - `strand` (Utf8, nullable): Strand (BED6)
fn determine_schema(
    bed_fields: BEDFields,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<SchemaRef> {
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::UInt16, true),
        Field::new("strand", DataType::Utf8, true),
    ];
    fields.truncate(bed_fields.count());
    // Add coordinate system metadata to schema
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    let schema = Schema::new_with_metadata(fields, metadata);
    debug!("Schema: {schema:?}");
    Ok(Arc::new(schema))
}

/// A DataFusion TableProvider for reading BED files
///
/// This struct implements the [`TableProvider`] trait to enable SQL queries over BED files.
/// It supports local and remote storage backends and streams a single partition
/// with configurable batch size and compression handling.
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
///     None,     // No cloud storage options
///     true,     // Use 0-based coordinates (default)
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
    /// Optional cloud storage configuration
    object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    coordinate_system_zero_based: bool,
}

impl BedTableProvider {
    /// Creates a new BED table provider
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the BED file (local filesystem or cloud storage URL)
    /// * `bed_fields` - BED format variant (BED3, BED4, BED5, BED6)
    /// * `object_storage_options` - Optional cloud storage configuration for remote files
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
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
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema(bed_fields, coordinate_system_zero_based)?;
        Ok(Self {
            file_path,
            bed_fields,
            schema,
            object_storage_options,
            coordinate_system_zero_based,
        })
    }
}

#[async_trait]
impl TableProvider for BedTableProvider {
    /// Returns `self` as `Any` for dynamic type casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the schema of the table
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the table type (always Base for BED files)
    fn table_type(&self) -> TableType {
        TableType::Base
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

        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };

        Ok(Arc::new(BedExec {
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            file_path: self.file_path.clone(),
            bed_fields: self.bed_fields,
            schema: schema.clone(),
            projection: projection.cloned(),
            limit,
            object_storage_options: self.object_storage_options.clone(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
        }))
    }
}
