//! Cooler DataFusion table provider.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use hdf5_metno::File;
use hdf5_metno::types::TypeDescriptor;

use crate::collection::{
    BinData, CoolerUri, ensure_local_path, load_bin_data, resolve_collection_group,
};
use crate::hdf5_utils::{h5_err, read_numeric_1d};
use crate::physical_exec::CoolerExec;

/// Table provider for local `.cool`/`.mcool` cooler files.
///
/// Exposes the pixels table of one data collection, either joined with bin
/// coordinates (`chrom1..count`, optionally `weight1`/`weight2`) or as the raw
/// COO triple (`bin1_id`, `bin2_id`, `count`).
pub struct CoolerTableProvider {
    file_path: String,
    group_path: String,
    schema: SchemaRef,
    join_bins: bool,
    include_weights: bool,
    count_is_float: bool,
    nnz: usize,
    coordinate_system_zero_based: bool,
    bin_cache: OnceLock<Arc<BinData>>,
}

impl Debug for CoolerTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoolerTableProvider")
            .field("file_path", &self.file_path)
            .field("group_path", &self.group_path)
            .field("join_bins", &self.join_bins)
            .field("include_weights", &self.include_weights)
            .field("nnz", &self.nnz)
            .finish()
    }
}

impl CoolerTableProvider {
    /// Open a cooler data collection.
    ///
    /// `path` accepts a plain file path or a cooler URI
    /// (`file.mcool::/resolutions/10000`); `resolution` selects an `.mcool`
    /// resolution when no URI group is given.
    pub fn new(
        path: String,
        resolution: Option<u64>,
        join_bins: bool,
        include_weights: bool,
        coordinate_system_zero_based: bool,
    ) -> Result<Self> {
        let uri = CoolerUri::parse(&path);
        let file_path = ensure_local_path(&uri.file_path)?;
        let file = File::open(&file_path)
            .map_err(|error| h5_err(&format!("Failed to open cooler file '{file_path}'"), error))?;
        let group_path = resolve_collection_group(&file, uri.group_path.as_deref(), resolution)?;
        let group = file
            .group(&group_path)
            .map_err(|error| h5_err(&format!("Failed to open group '{group_path}'"), error))?;

        let bins = group
            .group("bins")
            .map_err(|error| h5_err("Failed to open bins group", error))?;
        if include_weights {
            if !join_bins {
                return Err(DataFusionError::Plan(
                    "include_weights requires join_bins".to_string(),
                ));
            }
            if !bins.link_exists("weight") {
                return Err(DataFusionError::Plan(
                    "include_weights requested but this cooler has no bins/weight column (run `cooler balance` first)"
                        .to_string(),
                ));
            }
        }

        let count_ds = group
            .group("pixels")
            .and_then(|pixels| pixels.dataset("count"))
            .map_err(|error| h5_err("Failed to open pixels/count", error))?;
        let count_is_float = matches!(
            count_ds
                .dtype()
                .and_then(|dtype| dtype.to_descriptor())
                .map_err(|error| h5_err("Failed to read pixels/count dtype", error))?,
            TypeDescriptor::Float(_)
        );
        let nnz = count_ds.shape()[0];

        let schema = cooler_schema(
            join_bins,
            include_weights,
            count_is_float,
            coordinate_system_zero_based,
        );
        Ok(Self {
            file_path,
            group_path,
            schema,
            join_bins,
            include_weights,
            count_is_float,
            nnz,
            coordinate_system_zero_based,
            bin_cache: OnceLock::new(),
        })
    }

    /// Number of non-zero pixels (rows) in the collection.
    pub fn nnz(&self) -> usize {
        self.nnz
    }

    fn bin_data(&self) -> Result<Arc<BinData>> {
        if let Some(bins) = self.bin_cache.get() {
            return Ok(bins.clone());
        }
        let file = File::open(&self.file_path).map_err(|error| {
            h5_err(
                &format!("Failed to open cooler file '{}'", self.file_path),
                error,
            )
        })?;
        let group = file.group(&self.group_path).map_err(|error| {
            h5_err(
                &format!("Failed to open group '{}'", self.group_path),
                error,
            )
        })?;
        let bins = Arc::new(load_bin_data(&group, self.include_weights)?);
        Ok(self.bin_cache.get_or_init(|| bins).clone())
    }

    /// Split `0..nnz` into up to `target` contiguous row ranges aligned to
    /// bin1 boundaries (so future first-axis pruning composes with partitions).
    fn plan_partitions(&self, target: usize) -> Vec<(usize, usize)> {
        if self.nnz == 0 || target <= 1 {
            return vec![(0, self.nnz)];
        }
        let bin1_offset: Option<Vec<i64>> = File::open(&self.file_path)
            .and_then(|file| {
                file.group(&self.group_path)
                    .and_then(|group| group.group("indexes"))
                    .and_then(|indexes| indexes.dataset("bin1_offset"))
            })
            .ok()
            .and_then(|ds| read_numeric_1d::<i64>(&ds, "indexes/bin1_offset").ok());
        let mut boundaries = vec![0usize];
        for part in 1..target {
            let ideal = part * self.nnz / target;
            let aligned = match &bin1_offset {
                Some(offsets) => {
                    let index = offsets.partition_point(|&offset| (offset as usize) < ideal);
                    offsets
                        .get(index)
                        .map_or(self.nnz, |&offset| offset as usize)
                }
                None => ideal,
            };
            if aligned > *boundaries.last().expect("non-empty") && aligned < self.nnz {
                boundaries.push(aligned);
            }
        }
        boundaries.push(self.nnz);
        boundaries.windows(2).map(|w| (w[0], w[1])).collect()
    }
}

#[async_trait]
impl TableProvider for CoolerTableProvider {
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        // LIMIT is applied by DataFusion above this node.
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.schema, projection);
        // The bins/chroms join tables are only needed when a joined column is
        // actually projected; a bare `count` projection or `count(*)` skips them.
        let needs_bins =
            self.join_bins && schema.fields().iter().any(|field| field.name() != "count");
        let bins = if needs_bins {
            Some(self.bin_data()?)
        } else {
            None
        };
        let partitions = self.plan_partitions(state.config().target_partitions());
        let partition_count = partitions.len();
        Ok(Arc::new(CoolerExec {
            file_path: self.file_path.clone(),
            group_path: self.group_path.clone(),
            schema: schema.clone(),
            partitions,
            count_is_float: self.count_is_float,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            bins,
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(indices) => Arc::new(Schema::new_with_metadata(
            indices
                .iter()
                .map(|&index| schema.field(index).clone())
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        )),
        None => schema.clone(),
    }
}

fn cooler_schema(
    join_bins: bool,
    include_weights: bool,
    count_is_float: bool,
    coordinate_system_zero_based: bool,
) -> SchemaRef {
    let count_type = if count_is_float {
        DataType::Float64
    } else {
        DataType::Int32
    };
    let mut fields = if join_bins {
        vec![
            Field::new("chrom1", DataType::Utf8, false),
            Field::new("start1", DataType::UInt32, false),
            Field::new("end1", DataType::UInt32, false),
            Field::new("chrom2", DataType::Utf8, false),
            Field::new("start2", DataType::UInt32, false),
            Field::new("end2", DataType::UInt32, false),
            Field::new("count", count_type, false),
        ]
    } else {
        vec![
            Field::new("bin1_id", DataType::Int64, false),
            Field::new("bin2_id", DataType::Int64, false),
            Field::new("count", count_type, false),
        ]
    };
    if include_weights {
        // NaN marks bins filtered out by balancing, matching cooler's storage.
        fields.push(Field::new("weight1", DataType::Float64, false));
        fields.push(Field::new("weight2", DataType::Float64, false));
    }
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    Arc::new(Schema::new_with_metadata(fields, metadata))
}
