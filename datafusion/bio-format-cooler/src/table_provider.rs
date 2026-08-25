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
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use hdf5_metno::File;
use hdf5_metno::types::{FloatSize, IntSize, TypeDescriptor};

use crate::collection::{
    BinData, CoolerUri, CountType, IndexData, ensure_local_path, load_bin_data, load_index_data,
    resolve_collection_group,
};
use crate::fast_chunk::{FastPixels, index_column, validate_against_reference};
use crate::hdf5_utils::{h5_err, read_numeric_slice};
use crate::physical_exec::CoolerExec;
use crate::pruning::{is_first_axis_filter, plan_first_axis_ranges, plan_partitions};

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
    count_type: CountType,
    nnz: usize,
    coordinate_system_zero_based: bool,
    bin_cache: OnceLock<Arc<BinData>>,
    index_cache: OnceLock<Arc<IndexData>>,
    fast_cache: OnceLock<Arc<FastPixels>>,
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
        let count_descriptor = count_ds
            .dtype()
            .and_then(|dtype| dtype.to_descriptor())
            .map_err(|error| h5_err("Failed to read pixels/count dtype", error))?;
        let count_type = match count_descriptor {
            TypeDescriptor::Float(_) => CountType::Float64,
            TypeDescriptor::Integer(IntSize::U8) => CountType::Int64,
            TypeDescriptor::Unsigned(IntSize::U8) => CountType::UInt64,
            TypeDescriptor::Unsigned(IntSize::U4) => CountType::UInt32,
            TypeDescriptor::Integer(IntSize::U1 | IntSize::U2 | IntSize::U4)
            | TypeDescriptor::Unsigned(IntSize::U1 | IntSize::U2) => CountType::Int32,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported pixels/count dtype: {other}"
                )));
            }
        };
        let count_shape = count_ds.shape();
        if count_shape.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "pixels/count in '{group_path}' is not a 1-D dataset"
            )));
        }
        let nnz = count_shape[0];

        // Collection attributes surfaced as bio.cool.* schema metadata so the
        // Python layer can expose resolution/assembly without extra file reads.
        let mut extra_metadata: Vec<(String, String)> =
            vec![("bio.cool.group_path".to_string(), group_path.clone())];
        if let Some(bin_size) = crate::hdf5_utils::attr_i64(&group, "bin-size")? {
            extra_metadata.push(("bio.cool.resolution".to_string(), bin_size.to_string()));
        }
        if let Some(assembly) = crate::hdf5_utils::attr_string(&group, "genome-assembly")? {
            extra_metadata.push(("bio.cool.assembly".to_string(), assembly));
        }
        if let Some(version) = crate::hdf5_utils::attr_i64(&group, "format-version")? {
            extra_metadata.push(("bio.cool.format_version".to_string(), version.to_string()));
        }

        let schema = cooler_schema(
            join_bins,
            include_weights,
            count_type,
            coordinate_system_zero_based,
            &extra_metadata,
        );
        Ok(Self {
            file_path,
            group_path,
            schema,
            join_bins,
            include_weights,
            count_type,
            nnz,
            coordinate_system_zero_based,
            bin_cache: OnceLock::new(),
            index_cache: OnceLock::new(),
            fast_cache: OnceLock::new(),
        })
    }

    /// Number of non-zero pixels (rows) in the collection.
    pub fn nnz(&self) -> usize {
        self.nnz
    }

    fn bin_data(&self) -> Result<Arc<BinData>> {
        // Concurrent first scans may both load and one result wins the cache —
        // accepted trade-off: scan() runs once per query (partitions share the
        // Arc), so the duplicated load is bounded by concurrent queries, and
        // OnceLock keeps the fallible load out of get_or_init.
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

    fn index_data(&self) -> Result<Arc<IndexData>> {
        if let Some(index) = self.index_cache.get() {
            return Ok(index.clone());
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
        let index = Arc::new(load_index_data(&group)?);
        Ok(self.index_cache.get_or_init(|| index).clone())
    }
}

impl CoolerTableProvider {
    /// Build (once) the direct-chunk indexes for the pixel columns. Each
    /// column is validated against a libhdf5 reference read of its leading
    /// elements; any disagreement or unsupported layout falls back to the
    /// ordinary hdf5 read path for that column.
    fn fast_pixels(&self) -> Arc<FastPixels> {
        self.fast_cache
            .get_or_init(|| {
                if std::env::var_os("DATAFUSION_BIO_COOLER_DISABLE_FAST_PATH").is_some() {
                    return Arc::new(FastPixels::default());
                }
                // Fallback reasons are logged at debug level: a user seeing
                // lock-bound scan speeds can enable logging to learn why the
                // fast path is inactive.
                let build = || -> Result<FastPixels> {
                    let file = File::open(&self.file_path)
                        .map_err(|error| h5_err("Failed to open cooler file", error))?;
                    let pixels = file
                        .group(&self.group_path)
                        .and_then(|group| group.group("pixels"))
                        .map_err(|error| h5_err("Failed to open pixels group", error))?;
                    let indexed = |name: &str, expected: TypeDescriptor| -> Result<Option<_>> {
                        let ds = pixels.dataset(name).map_err(|error| {
                            h5_err(&format!("Failed to open pixels/{name}"), error)
                        })?;
                        let td = ds
                            .dtype()
                            .and_then(|dtype| dtype.to_descriptor())
                            .map_err(|error| h5_err("Failed to read dtype", error))?;
                        if td != expected {
                            return Ok(None);
                        }
                        let Some(column) = index_column(&ds) else {
                            return Ok(None);
                        };
                        let probe = column.chunk_elems.min(column.n_elems).min(8192);
                        let reference: Vec<u8> = match expected {
                            TypeDescriptor::Integer(IntSize::U8) => {
                                read_numeric_slice::<i64>(&ds, 0, probe, name)?
                                    .iter()
                                    .flat_map(|value| value.to_le_bytes())
                                    .collect()
                            }
                            TypeDescriptor::Integer(IntSize::U4) => {
                                read_numeric_slice::<i32>(&ds, 0, probe, name)?
                                    .iter()
                                    .flat_map(|value| value.to_le_bytes())
                                    .collect()
                            }
                            TypeDescriptor::Unsigned(IntSize::U8) => {
                                read_numeric_slice::<u64>(&ds, 0, probe, name)?
                                    .iter()
                                    .flat_map(|value| value.to_le_bytes())
                                    .collect()
                            }
                            TypeDescriptor::Unsigned(IntSize::U4) => {
                                read_numeric_slice::<u32>(&ds, 0, probe, name)?
                                    .iter()
                                    .flat_map(|value| value.to_le_bytes())
                                    .collect()
                            }
                            _ => read_numeric_slice::<f64>(&ds, 0, probe, name)?
                                .iter()
                                .flat_map(|value| value.to_le_bytes())
                                .collect(),
                        };
                        if !validate_against_reference(&column, &self.file_path, &reference) {
                            // Also covers byte order: the reference bytes are
                            // little-endian by construction, so a big-endian
                            // dataset fails the probe and stays on hdf5 reads.
                            log::debug!(
                                "cooler fast path disabled for pixels/{name}: reference probe mismatch"
                            );
                            return Ok(None);
                        }
                        Ok(Some(column))
                    };
                    let count_type = match self.count_type {
                        CountType::Float64 => TypeDescriptor::Float(FloatSize::U8),
                        CountType::Int64 => TypeDescriptor::Integer(IntSize::U8),
                        CountType::Int32 => TypeDescriptor::Integer(IntSize::U4),
                        CountType::UInt64 => TypeDescriptor::Unsigned(IntSize::U8),
                        CountType::UInt32 => TypeDescriptor::Unsigned(IntSize::U4),
                    };
                    Ok(FastPixels {
                        bin1: indexed("bin1_id", TypeDescriptor::Integer(IntSize::U8))?,
                        bin2: indexed("bin2_id", TypeDescriptor::Integer(IntSize::U8))?,
                        count: indexed("count", count_type)?,
                    })
                };
                Arc::new(build().unwrap_or_else(|error| {
                    log::debug!("cooler fast path disabled: {error}");
                    FastPixels::default()
                }))
            })
            .clone()
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                // Raw COO mode has no first-axis coordinate columns to prune on.
                if self.join_bins && is_first_axis_filter(expr) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // LIMIT is applied by DataFusion above this node.
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.schema, projection);
        // First-axis genomic filters prune the pixel row space through the
        // chrom_offset/bin1_offset CSR indexes; filters are Inexact, so
        // DataFusion re-applies them and pruning only needs to be a superset.
        let target_partitions = state.config().target_partitions();
        let has_first_axis_filter = self.join_bins && filters.iter().any(is_first_axis_filter);
        let ranges = if has_first_axis_filter {
            plan_first_axis_ranges(
                filters,
                self.coordinate_system_zero_based,
                self.bin_data()?.as_ref(),
                self.index_data()?.as_ref(),
                self.nnz,
            )?
        } else {
            vec![(0, self.nnz)]
        };
        // The bins/chroms join tables are only needed when a joined column is
        // actually projected; a bare `count` projection or `count(*)` skips them.
        let needs_bins =
            self.join_bins && schema.fields().iter().any(|field| field.name() != "count");
        let bins = if needs_bins {
            Some(self.bin_data()?)
        } else {
            None
        };
        let bin1_offset = if target_partitions > 1 {
            self.index_data().ok()
        } else {
            None
        };
        let partitions = plan_partitions(
            &ranges,
            target_partitions,
            bin1_offset
                .as_ref()
                .map(|index| index.bin1_offset.as_slice()),
        );
        let partition_count = partitions.len();
        Ok(Arc::new(CoolerExec {
            file_path: self.file_path.clone(),
            group_path: self.group_path.clone(),
            schema: schema.clone(),
            partitions,
            count_type: self.count_type,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            bins,
            fast: if schema.fields().is_empty() {
                // count(*) needs only the row ranges/nnz. Avoid visiting every
                // pixel chunk to build direct-read indexes for columns that the
                // execution plan will never open.
                Arc::new(FastPixels::default())
            } else {
                self.fast_pixels()
            },
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
    count_type: CountType,
    coordinate_system_zero_based: bool,
    extra_metadata: &[(String, String)],
) -> SchemaRef {
    let count_type = match count_type {
        CountType::Float64 => DataType::Float64,
        CountType::Int64 => DataType::Int64,
        CountType::Int32 => DataType::Int32,
        CountType::UInt64 => DataType::UInt64,
        CountType::UInt32 => DataType::UInt32,
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
    for (key, value) in extra_metadata {
        metadata.insert(key.clone(), value.clone());
    }
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::TableProvider;
    use datafusion::prelude::SessionContext;

    use super::CoolerTableProvider;

    #[tokio::test]
    async fn empty_projection_does_not_build_pixel_indexes() {
        let path = format!("{}/tests/data/test.cool", env!("CARGO_MANIFEST_DIR"));
        let provider = CoolerTableProvider::new(path, None, true, false, true).unwrap();
        let projection = Vec::new();
        let ctx = SessionContext::new();

        provider
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .unwrap();

        assert!(provider.fast_cache.get().is_none());
    }
}
