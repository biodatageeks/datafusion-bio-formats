//! Cooler DataFusion table provider.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, OnceLock};

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
    BinData, BinDataProjection, CoolerUri, CountType, IndexData, ensure_local_path, load_bin_data,
    load_index_data, resolve_collection_group,
};
use crate::fast_chunk::{ChunkedColumn, FastPixels, index_column, validate_against_reference};
use crate::hdf5_utils::{h5_err, read_numeric_slice};
use crate::physical_exec::CoolerExec;
use crate::pruning::{
    first_axis_pruning_projection, is_first_axis_filter, plan_first_axis_ranges, plan_partitions,
};

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
    bin_cache: Mutex<HashMap<u8, Arc<BinData>>>,
    index_cache: OnceLock<Arc<IndexData>>,
    fast_bin1_cache: OnceLock<Option<Arc<ChunkedColumn>>>,
    fast_bin2_cache: OnceLock<Option<Arc<ChunkedColumn>>>,
    fast_count_cache: OnceLock<Option<Arc<ChunkedColumn>>>,
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

        let pixels = group
            .group("pixels")
            .map_err(|error| h5_err("Failed to open pixels group", error))?;
        let bin1_ds = pixels
            .dataset("bin1_id")
            .map_err(|error| h5_err("Failed to open pixels/bin1_id", error))?;
        let bin2_ds = pixels
            .dataset("bin2_id")
            .map_err(|error| h5_err("Failed to open pixels/bin2_id", error))?;
        let count_ds = pixels
            .dataset("count")
            .map_err(|error| h5_err("Failed to open pixels/count", error))?;
        let nnz = validate_pixel_shapes(
            &group_path,
            &bin1_ds.shape(),
            &bin2_ds.shape(),
            &count_ds.shape(),
        )?;
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
            bin_cache: Mutex::new(HashMap::new()),
            index_cache: OnceLock::new(),
            fast_bin1_cache: OnceLock::new(),
            fast_bin2_cache: OnceLock::new(),
            fast_count_cache: OnceLock::new(),
        })
    }

    /// Number of non-zero pixels (rows) in the collection.
    pub fn nnz(&self) -> usize {
        self.nnz
    }

    fn bin_data(&self, projection: BinDataProjection) -> Result<Arc<BinData>> {
        let cache_key = projection.cache_key();
        if let Some(bins) = self
            .bin_cache
            .lock()
            .map_err(|_| DataFusionError::Internal("Cooler bin cache is poisoned".to_string()))?
            .get(&cache_key)
        {
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
        let bins = Arc::new(load_bin_data(&group, projection)?);
        Ok(self
            .bin_cache
            .lock()
            .map_err(|_| DataFusionError::Internal("Cooler bin cache is poisoned".to_string()))?
            .entry(cache_key)
            .or_insert_with(|| bins.clone())
            .clone())
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
    /// Build direct-chunk indexes only for pixel columns required by this
    /// projection. Each per-column result is cached independently, so later
    /// queries can add a newly needed column without revisiting the others.
    fn fast_pixels(&self, schema: &SchemaRef) -> Arc<FastPixels> {
        let needs_bin1 = schema.fields().iter().any(|field| {
            matches!(
                field.name().as_str(),
                "chrom1" | "start1" | "end1" | "weight1" | "bin1_id"
            )
        });
        let needs_bin2 = schema.fields().iter().any(|field| {
            matches!(
                field.name().as_str(),
                "chrom2" | "start2" | "end2" | "weight2" | "bin2_id"
            )
        });
        let needs_count = schema.fields().iter().any(|field| field.name() == "count");
        if std::env::var_os("DATAFUSION_BIO_COOLER_DISABLE_FAST_PATH").is_some() {
            return Arc::new(FastPixels::default());
        }

        let build_bin1 = needs_bin1 && self.fast_bin1_cache.get().is_none();
        let build_bin2 = needs_bin2 && self.fast_bin2_cache.get().is_none();
        let build_count = needs_count && self.fast_count_cache.get().is_none();
        if build_bin1 || build_bin2 || build_count {
            let built = self
                .build_fast_pixels(build_bin1, build_bin2, build_count)
                .unwrap_or_else(|error| {
                    log::debug!("cooler fast path disabled: {error}");
                    FastPixels::default()
                });
            if build_bin1 {
                let _ = self.fast_bin1_cache.set(built.bin1);
            }
            if build_bin2 {
                let _ = self.fast_bin2_cache.set(built.bin2);
            }
            if build_count {
                let _ = self.fast_count_cache.set(built.count);
            }
        }

        Arc::new(FastPixels {
            bin1: needs_bin1
                .then(|| self.fast_bin1_cache.get().cloned().flatten())
                .flatten(),
            bin2: needs_bin2
                .then(|| self.fast_bin2_cache.get().cloned().flatten())
                .flatten(),
            count: needs_count
                .then(|| self.fast_count_cache.get().cloned().flatten())
                .flatten(),
        })
    }

    fn build_fast_pixels(
        &self,
        needs_bin1: bool,
        needs_bin2: bool,
        needs_count: bool,
    ) -> Result<FastPixels> {
        let file = File::open(&self.file_path)
            .map_err(|error| h5_err("Failed to open cooler file", error))?;
        let pixels = file
            .group(&self.group_path)
            .and_then(|group| group.group("pixels"))
            .map_err(|error| h5_err("Failed to open pixels group", error))?;
        let indexed = |name: &str, expected: TypeDescriptor| -> Result<Option<_>> {
            let ds = pixels
                .dataset(name)
                .map_err(|error| h5_err(&format!("Failed to open pixels/{name}"), error))?;
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
            bin1: needs_bin1
                .then(|| indexed("bin1_id", TypeDescriptor::Integer(IntSize::U8)))
                .transpose()?
                .flatten(),
            bin2: needs_bin2
                .then(|| indexed("bin2_id", TypeDescriptor::Integer(IntSize::U8)))
                .transpose()?
                .flatten(),
            count: needs_count
                .then(|| indexed("count", count_type))
                .transpose()?
                .flatten(),
        })
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
        let pruning_projection = if self.join_bins {
            first_axis_pruning_projection(filters)
        } else {
            BinDataProjection::default()
        };
        let has_first_axis_filter = pruning_projection.any();
        let mut bin_projection = bin_projection(&schema);
        if has_first_axis_filter {
            bin_projection.chrom |= pruning_projection.chrom;
            bin_projection.start |= pruning_projection.start;
            bin_projection.end |= pruning_projection.end;
        }
        let bin_data = if self.join_bins && bin_projection.any() {
            Some(self.bin_data(bin_projection)?)
        } else {
            None
        };
        let ranges = if has_first_axis_filter {
            plan_first_axis_ranges(
                filters,
                self.coordinate_system_zero_based,
                bin_data.as_deref().expect("pruning bin data loaded"),
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
            Some(bin_data.expect("projected bin data loaded"))
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
                self.fast_pixels(&schema)
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

fn validate_pixel_shapes(
    group_path: &str,
    bin1_shape: &[usize],
    bin2_shape: &[usize],
    count_shape: &[usize],
) -> Result<usize> {
    for (name, shape) in [
        ("bin1_id", bin1_shape),
        ("bin2_id", bin2_shape),
        ("count", count_shape),
    ] {
        if shape.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "pixels/{name} in '{group_path}' is not a 1-D dataset"
            )));
        }
    }
    let nnz = count_shape[0];
    for (name, shape) in [("bin1_id", bin1_shape), ("bin2_id", bin2_shape)] {
        if shape[0] != nnz {
            return Err(DataFusionError::Plan(format!(
                "pixels/{name} in '{group_path}' has {} rows but pixels/count has {nnz}",
                shape[0]
            )));
        }
    }
    Ok(nnz)
}

fn bin_projection(schema: &SchemaRef) -> BinDataProjection {
    let mut projection = BinDataProjection::default();
    for field in schema.fields() {
        match field.name().as_str() {
            "chrom1" | "chrom2" => projection.chrom = true,
            "start1" | "start2" => projection.start = true,
            "end1" | "end2" => projection.end = true,
            "weight1" | "weight2" => projection.weight = true,
            _ => {}
        }
    }
    projection
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
            Field::new("start1", DataType::UInt64, false),
            Field::new("end1", DataType::UInt64, false),
            Field::new("chrom2", DataType::Utf8, false),
            Field::new("start2", DataType::UInt64, false),
            Field::new("end2", DataType::UInt64, false),
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

    use super::{CoolerTableProvider, validate_pixel_shapes};
    use crate::physical_exec::CoolerExec;

    #[test]
    fn rejects_mismatched_or_non_vector_pixel_arrays() {
        let mismatch = validate_pixel_shapes("/", &[2], &[3], &[2])
            .unwrap_err()
            .to_string();
        assert!(mismatch.contains("pixels/bin2_id"), "{mismatch}");
        assert!(mismatch.contains("3 rows"), "{mismatch}");

        let matrix = validate_pixel_shapes("/", &[1, 2], &[2], &[2])
            .unwrap_err()
            .to_string();
        assert!(matrix.contains("pixels/bin1_id"), "{matrix}");
        assert!(matrix.contains("not a 1-D dataset"), "{matrix}");
    }

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

        assert!(provider.fast_bin1_cache.get().is_none());
        assert!(provider.fast_bin2_cache.get().is_none());
        assert!(provider.fast_count_cache.get().is_none());
    }

    #[tokio::test]
    async fn projected_scan_only_builds_needed_pixel_indexes() {
        let path = format!("{}/tests/data/test.cool", env!("CARGO_MANIFEST_DIR"));
        let count_provider =
            CoolerTableProvider::new(path.clone(), None, true, false, true).unwrap();
        let ctx = SessionContext::new();
        count_provider
            .scan(&ctx.state(), Some(&vec![6]), &[], None)
            .await
            .unwrap();
        assert!(count_provider.fast_bin1_cache.get().is_none());
        assert!(count_provider.fast_bin2_cache.get().is_none());
        assert!(count_provider.fast_count_cache.get().is_some());

        let chrom1_provider = CoolerTableProvider::new(path, None, true, false, true).unwrap();
        chrom1_provider
            .scan(&ctx.state(), Some(&vec![0]), &[], None)
            .await
            .unwrap();
        assert!(chrom1_provider.fast_bin1_cache.get().is_some());
        assert!(chrom1_provider.fast_bin2_cache.get().is_none());
        assert!(chrom1_provider.fast_count_cache.get().is_none());
    }

    #[tokio::test]
    async fn projected_scan_only_loads_needed_bin_metadata() {
        let path = format!("{}/tests/data/test.cool", env!("CARGO_MANIFEST_DIR"));
        let ctx = SessionContext::new();

        let chrom_provider =
            CoolerTableProvider::new(path.clone(), None, true, false, true).unwrap();
        let chrom_plan = chrom_provider
            .scan(&ctx.state(), Some(&vec![0]), &[], None)
            .await
            .unwrap();
        let chrom_bins = chrom_plan
            .as_any()
            .downcast_ref::<CoolerExec>()
            .unwrap()
            .bins
            .as_ref()
            .unwrap();
        assert!(!chrom_bins.chrom_names.is_empty());
        assert!(!chrom_bins.chrom_idx.is_empty());
        assert!(chrom_bins.start.is_empty());
        assert!(chrom_bins.end.is_empty());
        assert!(chrom_bins.weight.is_none());

        let start_provider = CoolerTableProvider::new(path, None, true, false, true).unwrap();
        let start_plan = start_provider
            .scan(&ctx.state(), Some(&vec![1]), &[], None)
            .await
            .unwrap();
        let start_bins = start_plan
            .as_any()
            .downcast_ref::<CoolerExec>()
            .unwrap()
            .bins
            .as_ref()
            .unwrap();
        assert!(start_bins.chrom_names.is_empty());
        assert!(start_bins.chrom_idx.is_empty());
        assert!(!start_bins.start.is_empty());
        assert!(start_bins.end.is_empty());
        assert!(start_bins.weight.is_none());
    }
}
